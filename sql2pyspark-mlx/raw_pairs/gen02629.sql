WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('-', firmware, device_code) AS firmware_device_code_key
    FROM device_readings
    WHERE temp_c >= 26
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_device_code_key,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    session_id,
    CORR(status_bits, pressure) AS corr_status_bits_pressure,
    AVG(humidity) AS avg_humidity,
    STDDEV_SAMP(status_bits) AS status_bits_stddev,
    MAX(reading_ts) AS last_reading_ts
FROM sessions_device_readings
GROUP BY session_id
ORDER BY status_bits_stddev;
