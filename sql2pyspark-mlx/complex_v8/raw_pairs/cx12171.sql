WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        ticket_id,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_no
)
-- Final projection
SELECT
    ticket_id,
    session_no,
    session_start,
    session_end,
    n_events,
    duration_min
FROM per_session
ORDER BY session_start NULLS LAST, session_end NULLS LAST, ticket_id, session_no
LIMIT 5
