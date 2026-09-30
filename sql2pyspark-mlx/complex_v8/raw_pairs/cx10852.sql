WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 79.4
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, burst_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM per_session
    WHERE burst_id < (SELECT MAX(burst_id) FROM per_session)
),
tagged AS (
    -- Derive helper columns
    SELECT
        quality,
        burst_id,
        session_start,
        session_end,
        events,
        duration_min,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        QUARTER(session_start) AS session_start_quarter,
        ROUND(events / NULLIF(burst_id, 0), 3) AS events_per_burst_id
    FROM qualified
)
-- Final projection
SELECT
    quality,
    burst_id,
    session_end,
    events,
    quality_or_unknown,
    events_per_burst_id
FROM tagged
ORDER BY events DESC, events_per_burst_id DESC
