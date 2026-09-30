WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(humidity, 0) AS humidity_rounded
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 09:00:00')
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    SUM(CASE WHEN device_code = 'DV-02' THEN temp_c ELSE 0 END) AS dv_02_temp_c,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events,
    APPROX_PERCENTILE(temp_c, 0.95) AS temp_c_p95
FROM cleaned
GROUP BY DATE_TRUNC('DAY', reading_ts)
ORDER BY temp_c_p95 DESC
