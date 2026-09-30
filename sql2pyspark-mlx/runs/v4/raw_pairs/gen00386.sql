WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(humidity, 0) AS humidity_filled,
        pressure + status_bits AS pressure_plus_status_bits
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 05:00:00'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Grouped metrics
SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    CORR(pressure, status_bits) AS corr_pressure_status_bits
FROM islands
GROUP BY device_code, DATE_TRUNC('DAY', reading_ts)
ORDER BY corr_pressure_status_bits DESC
