WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN duration_min >= 192.0 THEN 'large' WHEN duration_min >= 60.0 THEN 'medium' ELSE 'small' END AS duration_min_tier,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end,
        COALESCE(session_id, 0) AS session_id_filled
    FROM sessions
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_events, duration_min_tier, duration_min
    FROM derived
) src
PIVOT (
    SUM(duration_min) AS amount, COUNT(duration_min) AS n
    FOR duration_min_tier IN ('large', 'small', 'medium')
)
ORDER BY n_events
