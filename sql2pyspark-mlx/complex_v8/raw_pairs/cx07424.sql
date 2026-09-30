WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN actor = 'system' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM matched s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_3d,
        s.prior_requester_id_3d,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('replied', 'resolved')
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_3d,
    event_type,
    event_at,
    ABS(prior_rows_3d - 4) AS prior_rows_3d_distance,
    DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
FROM detail_rows
ORDER BY prior_rows_3d_distance DESC
