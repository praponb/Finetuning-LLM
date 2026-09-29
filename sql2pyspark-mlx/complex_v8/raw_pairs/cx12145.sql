WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(event_type_len) AS session_event_type_len,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
grouped AS (
    -- Group-level totals
    SELECT
        event_type,
        SUM(duration_min) FILTER (WHERE event_type <> 'escalated') AS duration_min_event_type_escalated,
        SUM(duration_min) AS total_duration_min,
        PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id,
        COUNT(*) AS n_rows
    FROM per_session
    GROUP BY 1
    HAVING SUM(session_event_type_len) > 24
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE total_duration_min > (SELECT AVG(total_duration_min) FROM grouped)
)
SELECT
    event_type,
    total_duration_min,
    n_rows,
    COALESCE(n_rows, 0) AS n_rows_filled
FROM screened
ORDER BY event_type DESC
