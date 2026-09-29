WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(read_at_hour) AS session_read_at_hour,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
)
SELECT
    site_id,
    session_start,
    session_end,
    event_count,
    session_read_at_hour,
    duration_min,
    ROUND(duration_min * 0.25, 2) AS duration_min_adj,
    DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start
FROM per_session
ORDER BY event_count
