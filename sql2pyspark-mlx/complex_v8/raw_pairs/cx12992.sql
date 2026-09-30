WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, burst_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        MAX(session_start) AS last_session_start,
        COUNT(*) AS row_count,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        MIN(duration_min) AS min_duration_min,
        COUNT(*) FILTER (WHERE quality = 'EST') AS n_quality_est
    FROM session_summary
    GROUP BY 1
    HAVING MAX(n_events) > 2
),
derived AS (
    -- Add calculated columns
    SELECT
        quality,
        distinct_qualitys,
        min_duration_min,
        DATEDIFF(DATE '2026-01-01', last_session_start) AS days_since_last_session_start,
        DATE_TRUNC('WEEK', last_session_start) AS last_session_start_week,
        LEAST(distinct_qualitys, n_quality_est) AS least_distinct_qualitys_n_quality_est
    FROM grouped
)
SELECT
    quality,
    distinct_qualitys,
    min_duration_min,
    last_session_start_week,
    least_distinct_qualitys_n_quality_est,
    distinct_qualitys + min_duration_min AS distinct_qualitys_plus_min_duration_min,
    DATE_TRUNC('HOUR', last_session_start_week) AS last_session_start_week_hour
FROM derived
ORDER BY distinct_qualitys_plus_min_duration_min, quality DESC
