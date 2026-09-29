WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, burst_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        burst_id,
        site_id,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        ROUND(STDDEV(duration_min), 2) AS stddev_duration_min,
        MIN(session_end) AS first_session_end
    FROM per_session
    WHERE burst_id > 3
    GROUP BY 1, 2, 3
    HAVING MAX(duration_min) > 24.0
)
SELECT
    burst_id,
    site_id,
    GROUPING_ID() AS grouping_level,
    PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id,
    ROUND(SUM(burst_id) / NULLIF(SUM(stddev_duration_min), 0), 3) AS burst_id_to_stddev_duration_min_ratio,
    MAX(session_start_month) AS last_session_start_month
FROM summary
GROUP BY GROUPING SETS ((burst_id, site_id), (burst_id))
ORDER BY burst_id NULLS LAST, site_id NULLS LAST
