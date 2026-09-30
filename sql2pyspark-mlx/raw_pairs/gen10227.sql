WITH filtered AS (
    SELECT *, CONCAT(firmware, ' ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    SUM(burst_id) AS total_burst_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    MIN(temp_c) AS min_temp_c,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    APPROX_PERCENTILE(temp_c, 0.95) AS temp_c_p95
FROM sessions_device_readings
GROUP BY DATE_TRUNC('DAY', reading_ts)
ORDER BY total_burst_id DESC;
