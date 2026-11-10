package workersvc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/vtmocanu/uzi/api/internal/capability"
	"github.com/vtmocanu/uzi/api/internal/pgconv"
	"github.com/vtmocanu/uzi/api/internal/store"
)

// Issue #4044: a queued continuation whose assigned worker lacks repo_agent_source_v1 is
// refused by ClaimRun for that worker; health must say so instead of the owner-hold or
// all-workers-busy text.

const (
	gapFragmentText = "currently lacks the required protocol capability repo_agent_source_v1"
	gapPostHold     = "its assigned worker w-old " + gapFragmentText +
		", so it can resume this run only after re-registering with it; "
	gapTailNoClaim = "no other worker can claim it right now"
	gapTailPickup  = "waiting for another worker to pick it up"
)

func isAssignedCapabilityGapReason(reason string) bool {
	return strings.HasPrefix(reason, assignedWorkerPrefix+" ") && strings.Contains(reason, repoAgentSourceGapFragment)
}

type assignedGapStore struct {
	*healthFakeStore
	capable   int64
	worker    store.Worker
	workerErr error
	getCalls  []store.GetWorkerByIDForUserParams
}

func (f *assignedGapStore) CountOnlineWorkersSatisfyingRepoAgentSource(context.Context, uuid.UUID) (int64, error) {
	return f.capable, nil
}

func (f *assignedGapStore) GetWorkerByIDForUser(_ context.Context, arg store.GetWorkerByIDForUserParams) (store.Worker, error) {
	f.getCalls = append(f.getCalls, arg)
	return f.worker, f.workerErr
}

// assignedGapRun is the observed shape: queued past the threshold and past the 2h affinity
// ceiling, assigned to a worker, requiring repo agent source.
func assignedGapRun() store.ListActiveRunsForHealthRow {
	r := queuedRunPastThreshold()
	r.WorkerID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	r.RepoAgentSourceRequired = true
	r.RepoAgentSourceCommit = pgtype.Text{String: "1111111111111111111111111111111111111111", Valid: true}
	r.UpdatedAt = ago(3 * time.Hour)
	return r
}

func assignedGapSetup(r store.ListActiveRunsForHealthRow) (*assignedGapStore, *Service, *fakeBroadcaster) {
	gs := &assignedGapStore{
		healthFakeStore: &healthFakeStore{
			active:          []store.ListActiveRunsForHealthRow{r},
			onlineWorkers:   1,
			freeSlotWorkers: 0,
		},
		capable: 1,
		worker: store.Worker{
			ID:              uuid.UUID(r.WorkerID.Bytes),
			UserID:          r.UserID,
			Name:            "w-old",
			LastHeartbeatAt: pgconv.Time(t0),
		},
	}
	svc := healthSvc(gs, defaultHealthSettings())
	svc.p.WorkerAffinityCeiling = 2 * time.Hour
	svc.p.WorkerStaleRequeueGrace = 0
	b := &fakeBroadcaster{}
	svc.SetBroadcaster(b)
	return gs, svc, b
}

func assignedGapReason(t *testing.T, gs *assignedGapStore, svc *Service, r store.ListActiveRunsForHealthRow) string {
	t.Helper()
	svc.detectRunHealth(context.Background(), t0)
	return lastWrite(t, gs.healthFakeStore, r.ID).HealthReason.String
}

func TestHealthAssignedRepoAgentSourceGapObservedShape(t *testing.T) {
	r := assignedGapRun()
	gs, svc, _ := assignedGapSetup(r)
	got := assignedGapReason(t, gs, svc, r)
	want := gapPostHold + gapTailNoClaim
	if got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
	if strings.Contains(got, "all your workers are busy") || strings.Contains(got, "w-old can claim") {
		t.Fatalf("reason %q still blames busy workers or claims the assigned worker can run it", got)
	}
	if len(gs.getCalls) != 1 || gs.getCalls[0].ID != uuid.UUID(r.WorkerID.Bytes) || gs.getCalls[0].UserID != r.UserID {
		t.Fatalf("owner-scoped lookup params = %+v", gs.getCalls)
	}
}

func TestHealthAssignedRepoAgentSourceGapPostHoldTails(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*assignedGapStore)
		tail string
	}{
		{"claimable elsewhere", func(g *assignedGapStore) { g.eligibility.Claimable = 1 }, gapTailPickup},
		{"eligibility error", func(g *assignedGapStore) { g.eligibilityErr = errors.New("boom") }, gapTailPickup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := assignedGapRun()
			gs, svc, _ := assignedGapSetup(r)
			tc.set(gs)
			if got, want := assignedGapReason(t, gs, svc, r), gapPostHold+tc.tail; got != want {
				t.Fatalf("reason = %q, want %q", got, want)
			}
		})
	}
}

func TestHealthAssignedRepoAgentSourceGapHeldVariants(t *testing.T) {
	const heldSuffix = ": it " + gapFragmentText +
		", so it can resume this run only after re-registering with it; another worker may take it after that"
	t.Run("owner hold", func(t *testing.T) {
		r := assignedGapRun()
		r.OwnerHoldPinnable = true
		r.StaleRequeueWorkerName = "w-old"
		r.UpdatedAt = ago(30 * time.Minute)
		gs, svc, _ := assignedGapSetup(r)
		svc.p.WorkerAffinityCeiling = time.Hour
		got := assignedGapReason(t, gs, svc, r)
		want := "waiting until 2026-07-12T12:30:00Z for its previous worker w-old" + heldSuffix
		if got != want || !isStaleRequeuePinReason(got) || !strings.HasPrefix(got, "waiting until ") {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	t.Run("owner hold without name", func(t *testing.T) {
		r := assignedGapRun()
		r.OwnerHoldPinnable = true
		r.UpdatedAt = ago(30 * time.Minute)
		gs, svc, _ := assignedGapSetup(r)
		gs.worker.Name = ""
		svc.p.WorkerAffinityCeiling = time.Hour
		got := assignedGapReason(t, gs, svc, r)
		want := "waiting until 2026-07-12T12:30:00Z for its previous worker" + heldSuffix
		if got != want || !isStaleRequeuePinReason(got) {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	t.Run("live non-draining owner", func(t *testing.T) {
		r := assignedGapRun()
		r.StaleRequeueWorkerName = "w-old"
		r.UpdatedAt = ago(30 * time.Minute)
		gs, svc, _ := assignedGapSetup(r)
		got := assignedGapReason(t, gs, svc, r)
		want := "waiting until 2026-07-12T13:30:00Z for its previous worker w-old" + heldSuffix
		if got != want || !isStaleRequeuePinReason(got) {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	// ClaimRun's ceiling is inclusive (updated_at >= @affinity_cutoff): exactly at it, still held.
	t.Run("live owner at the ceiling", func(t *testing.T) {
		r := assignedGapRun()
		r.StaleRequeueWorkerName = "w-old"
		r.UpdatedAt = ago(2 * time.Hour)
		gs, svc, _ := assignedGapSetup(r)
		got := assignedGapReason(t, gs, svc, r)
		want := "waiting until 2026-07-12T12:00:00Z for its previous worker w-old" + heldSuffix
		if got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	for _, tc := range []struct {
		name string
		mut  func(*store.ListActiveRunsForHealthRow, *assignedGapStore)
	}{
		{"no owner projection", func(r *store.ListActiveRunsForHealthRow, _ *assignedGapStore) { r.StaleRequeueWorkerName = "" }},
		{"ceiling passed", func(r *store.ListActiveRunsForHealthRow, _ *assignedGapStore) { r.UpdatedAt = ago(3 * time.Hour) }},
		{"stale heartbeat", func(_ *store.ListActiveRunsForHealthRow, g *assignedGapStore) {
			g.worker.LastHeartbeatAt = ago(time.Hour)
		}},
		{"no heartbeat", func(_ *store.ListActiveRunsForHealthRow, g *assignedGapStore) {
			g.worker.LastHeartbeatAt = pgtype.Timestamptz{}
		}},
	} {
		t.Run("post-hold "+tc.name, func(t *testing.T) {
			r := assignedGapRun()
			r.StaleRequeueWorkerName = "w-old"
			r.UpdatedAt = ago(30 * time.Minute)
			gs, svc, _ := assignedGapSetup(r)
			tc.mut(&r, gs)
			gs.active = []store.ListActiveRunsForHealthRow{r}
			got := assignedGapReason(t, gs, svc, r)
			if !isAssignedCapabilityGapReason(got) || isStaleRequeuePinReason(got) {
				t.Fatalf("reason = %q, want a post-hold gap reason", got)
			}
		})
	}
}

func TestHealthAssignedRepoAgentSourceGapBeatsPriorityRelabel(t *testing.T) {
	r := assignedGapRun()
	gs, svc, _ := assignedGapSetup(r)
	gs.priorityClass = map[uuid.UUID]string{r.ID: "background"}
	if got := assignedGapReason(t, gs, svc, r); got != gapPostHold+gapTailNoClaim {
		t.Fatalf("reason = %q, want the gap reason over deprioritized", got)
	}
	// Control: with no gap the same run is labelled deprioritized.
	r2 := assignedGapRun()
	r2.WorkerID = pgtype.UUID{}
	gs2, svc2, _ := assignedGapSetup(r2)
	gs2.priorityClass = map[uuid.UUID]string{r2.ID: "background"}
	if got := assignedGapReason(t, gs2, svc2, r2); got != reasonDeprioritized {
		t.Fatalf("control reason = %q, want %q", got, reasonDeprioritized)
	}
}

func TestHealthAssignedRepoAgentSourceGapFailsOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*assignedGapStore, store.ListActiveRunsForHealthRow)
	}{
		{"read error", func(g *assignedGapStore, _ store.ListActiveRunsForHealthRow) { g.workerErr = errors.New("boom") }},
		{"id mismatch", func(g *assignedGapStore, _ store.ListActiveRunsForHealthRow) { g.worker.ID = uuid.New() }},
		{"wrong owner", func(g *assignedGapStore, _ store.ListActiveRunsForHealthRow) { g.worker.UserID = uuid.New() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := assignedGapRun()
			gs, svc, _ := assignedGapSetup(r)
			gs.worker.Name = "foreign-name"
			tc.mut(gs, r)
			got := assignedGapReason(t, gs, svc, r)
			if got != reasonAllWorkersBusy || strings.Contains(got, "foreign-name") {
				t.Fatalf("reason = %q, want %q", got, reasonAllWorkersBusy)
			}
			if len(gs.getCalls) != 1 || gs.getCalls[0].UserID != r.UserID || gs.getCalls[0].ID != uuid.UUID(r.WorkerID.Bytes) {
				t.Fatalf("lookup params = %+v, want owner-scoped to run user and worker", gs.getCalls)
			}
		})
	}
}

func TestHealthAssignedRepoAgentSourceCapableOwnerUnchanged(t *testing.T) {
	r := assignedGapRun()
	gs, svc, _ := assignedGapSetup(r)
	gs.worker.ProtocolCapabilities = []string{capability.RepoAgentSourceV1}
	if got := assignedGapReason(t, gs, svc, r); got != reasonAllWorkersBusy {
		t.Fatalf("reason = %q, want %q", got, reasonAllWorkersBusy)
	}
}

func TestHealthAssignedRepoAgentSourceNoWorkerOrRequirementNoLookup(t *testing.T) {
	r := assignedGapRun()
	r.WorkerID = pgtype.UUID{}
	gs, svc, _ := assignedGapSetup(r)
	if got := assignedGapReason(t, gs, svc, r); got != reasonAllWorkersBusy {
		t.Fatalf("reason = %q, want %q", got, reasonAllWorkersBusy)
	}
	if len(gs.getCalls) != 0 {
		t.Fatalf("looked up a worker for a run with no worker_id: %+v", gs.getCalls)
	}

	r = assignedGapRun()
	r.RepoAgentSourceRequired = false
	r.RepoAgentSourceCommit = pgtype.Text{}
	gs, svc, _ = assignedGapSetup(r)
	if got := assignedGapReason(t, gs, svc, r); got != reasonAllWorkersBusy {
		t.Fatalf("reason = %q, want %q", got, reasonAllWorkersBusy)
	}
	if len(gs.getCalls) != 0 {
		t.Fatalf("looked up a worker for a run without the requirement: %+v", gs.getCalls)
	}
}

func TestHealthAssignedRepoAgentSourceGapStrongerReasonsWin(t *testing.T) {
	t.Run("vault locked", func(t *testing.T) {
		r := assignedGapRun()
		gs, svc, _ := assignedGapSetup(r)
		svc.SetVault(newMemoryVault(newBox(t), newMemVaultStore()))
		if got := assignedGapReason(t, gs, svc, r); got != reasonVaultLocked {
			t.Fatalf("reason = %q, want %q", got, reasonVaultLocked)
		}
	})
	t.Run("reserve hold", func(t *testing.T) {
		r := assignedGapRun()
		r.ReserveHoldReason = "weekly reserve text"
		gs, svc, _ := assignedGapSetup(r)
		if got := assignedGapReason(t, gs, svc, r); got != "weekly reserve text" {
			t.Fatalf("reason = %q, want the reserve text", got)
		}
	})
	t.Run("custody limit", func(t *testing.T) {
		r := assignedGapRun()
		gs, svc, _ := assignedGapSetup(r)
		gs.custodyHolds = int64(testCustodyLimit)
		th := healthThresholds{queued: 10 * time.Minute, custodyLimit: testCustodyLimit, custodyLimitOK: true}
		if _, got := svc.healthTargetFor(context.Background(), t0, r, th); got != reasonCustodyLimit {
			t.Fatalf("reason = %q, want %q", got, reasonCustodyLimit)
		}
	})
	t.Run("no capable repo agent source worker", func(t *testing.T) {
		r := assignedGapRun()
		gs, svc, _ := assignedGapSetup(r)
		gs.capable = 0
		if got := assignedGapReason(t, gs, svc, r); got != reasonNoRepoAgentSourceCapableWorker {
			t.Fatalf("reason = %q, want %q", got, reasonNoRepoAgentSourceCapableWorker)
		}
	})
}

// Pin lifecycle (i): the held variant is a pin reason, so a young run shows it and no nudge fires.
func TestHealthAssignedRepoAgentSourceHeldNoNudgeWhenYoung(t *testing.T) {
	r := assignedGapRun()
	r.StatusSince = ago(time.Minute)
	r.OwnerHoldPinnable = true
	r.StaleRequeueWorkerName = "w-old"
	r.UpdatedAt = ago(30 * time.Minute)
	gs, svc, b := assignedGapSetup(r)
	svc.p.WorkerAffinityCeiling = time.Hour
	svc.detectRunHealth(context.Background(), t0)
	w := lastWrite(t, gs.healthFakeStore, r.ID)
	if w.Health != healthWaitingWorker || !isStaleRequeuePinReason(w.HealthReason.String) || !strings.Contains(w.HealthReason.String, gapFragmentText) {
		t.Fatalf("write = (%q, %q), want waiting_worker with the held gap reason", w.Health, w.HealthReason.String)
	}
	if w.HealthNotifiedAt.Valid || len(b.healthNudges) != 1 || b.healthNudges[0] {
		t.Fatalf("held gap reason nudged: stamp=%v nudges=%v", w.HealthNotifiedAt, b.healthNudges)
	}
}

// Pin lifecycle (ii): the post-hold variant is a genuine wait and nudges from ok.
func TestHealthAssignedRepoAgentSourcePostHoldNudgesFromOK(t *testing.T) {
	r := assignedGapRun()
	gs, svc, b := assignedGapSetup(r)
	svc.detectRunHealth(context.Background(), t0)
	w := lastWrite(t, gs.healthFakeStore, r.ID)
	if w.Health != healthWaitingWorker || !isAssignedCapabilityGapReason(w.HealthReason.String) {
		t.Fatalf("write = (%q, %q), want waiting_worker with the post-hold gap reason", w.Health, w.HealthReason.String)
	}
	if !w.HealthNotifiedAt.Valid || len(b.healthNudges) != 1 || !b.healthNudges[0] {
		t.Fatalf("post-hold gap reason did not nudge: stamp=%v nudges=%v", w.HealthNotifiedAt, b.healthNudges)
	}
}

// Pin lifecycle (iii): leaving the held variant for the post-hold one is a new episode.
func TestHealthAssignedRepoAgentSourceLeavingHoldRestampsAndNudges(t *testing.T) {
	r := assignedGapRun()
	r.Health = healthWaitingWorker
	r.HealthReason = pgtype.Text{String: "waiting until 2026-07-12T11:00:00Z for its previous worker w-old: it " + gapFragmentText +
		", so it can resume this run only after re-registering with it; another worker may take it after that", Valid: true}
	r.HealthSince = ago(2 * time.Hour)
	gs, svc, b := assignedGapSetup(r)
	svc.detectRunHealth(context.Background(), t0)
	w := lastWrite(t, gs.healthFakeStore, r.ID)
	if !isAssignedCapabilityGapReason(w.HealthReason.String) {
		t.Fatalf("reason = %q, want the post-hold gap reason", w.HealthReason.String)
	}
	if !w.HealthSince.Time.Equal(t0) {
		t.Fatalf("health_since = %v, want restamped to %v", w.HealthSince.Time, t0)
	}
	if !w.HealthNotifiedAt.Valid || len(b.healthNudges) != 1 || !b.healthNudges[0] {
		t.Fatalf("leaving the hold did not nudge: stamp=%v nudges=%v", w.HealthNotifiedAt, b.healthNudges)
	}
}
