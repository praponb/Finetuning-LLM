WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
)
-- Final projection
SELECT
    site_id,
    session_no,
    session_start,
    session_end,
    n_events,
    session_voltage,
    FLOOR(session_voltage / 100) * 100 AS session_voltage_bucket,
    YEAR(session_end) AS session_end_year
FROM per_session
ORDER BY session_end_year
