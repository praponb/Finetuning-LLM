WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 109.3 THEN 'large' WHEN kwh >= 30.2 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM meter_readings
    WHERE quality = 'BAD'
),
with_prev AS (
    -- Previous read_at per kwh_tier
    SELECT *, LAG(read_at) OVER (PARTITION BY kwh_tier ORDER BY read_at, id) AS prev_read_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY kwh_tier ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        kwh_tier,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY kwh_tier, session_id
)
-- Final projection
SELECT
    kwh_tier,
    session_id,
    session_start,
    session_end,
    event_count,
    duration_min,
    TO_DATE(session_start) AS session_start_date,
    TO_DATE(session_end) AS session_end_date
FROM per_session
