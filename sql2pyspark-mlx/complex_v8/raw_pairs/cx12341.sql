WITH scoped AS (
    -- Filter ticket_events
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
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
enriched AS (
    -- Label and score rows
    SELECT
        ticket_id,
        session_id,
        session_start,
        session_end,
        n_events,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket,
        TO_DATE(session_start) AS session_start_date,
        ROUND(duration_min * 1.07, 2) AS duration_min_adj
    FROM sessions
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        session_id,
        YEAR(session_start_date) AS session_start_date_year,
        ticket_id,
        MIN(n_events) AS min_n_events,
        MIN(session_id) AS min_session_id,
        SUM(duration_min_bucket) AS total_duration_min_bucket
    FROM enriched
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    session_id,
    session_start_date_year,
    min_n_events,
    min_session_id,
    total_duration_min_bucket,
    COALESCE(session_id, 0) AS session_id_filled
FROM per_group
WHERE session_start_date_year BETWEEN 2024 AND 2025
ORDER BY min_session_id
