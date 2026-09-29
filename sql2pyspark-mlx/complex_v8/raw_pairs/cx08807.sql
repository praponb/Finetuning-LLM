WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'customer')
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
scored AS (
    -- Label and score rows
    SELECT
        event_type,
        burst_id,
        session_end,
        duration_min,
        LOWER(TRIM(event_type)) AS event_type_clean,
        COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM session_summary
    WHERE MONTH(session_start) IN (2, 3)
)
-- Final projection
SELECT
    event_type,
    session_end,
    duration_min,
    event_type_clean,
    duration_min_filled
FROM scored
