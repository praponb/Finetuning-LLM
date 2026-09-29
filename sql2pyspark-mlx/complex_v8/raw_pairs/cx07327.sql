WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
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
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        ROUND(SUM(session_voltage) / NULLIF(SUM(burst_id), 0), 3) AS session_voltage_to_burst_id_ratio,
        ROUND(SUM(burst_id) / NULLIF(SUM(duration_min), 0), 3) AS burst_id_to_duration_min_ratio,
        PERCENTILE_APPROX(event_count, 0.5) AS median_event_count
    FROM session_summary
    GROUP BY DATE_TRUNC('MONTH', session_start)
)
-- Final projection
SELECT
    session_start_month,
    session_voltage_to_burst_id_ratio,
    burst_id_to_duration_min_ratio
FROM totals
ORDER BY session_voltage_to_burst_id_ratio, session_start_month
