WITH base AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        ticket_id,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, burst_id
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM per_session s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority = 'P1'
        )
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'agent'
              AND TO_DATE(t.event_at) < DATE '2025-03-14'
        )
      AND s.ticket_id IS NOT NULL
)
-- Final projection
SELECT
    ticket_id,
    burst_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    ROUND(duration_min * 0.25, 2) AS duration_min_adj,
    ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end
FROM without_matches
ORDER BY minutes_session_start_to_session_end DESC
