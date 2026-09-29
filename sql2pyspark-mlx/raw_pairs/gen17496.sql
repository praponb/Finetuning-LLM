WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SUM(humidity) AS total_humidity,
    MIN(reading_ts) AS first_reading_ts,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    PERCENTILE_APPROX(pressure, 0.95) AS pressure_p95,
    RANK() OVER (PARTITION BY device_code ORDER BY SUM(humidity) DESC) AS total_humidity_rank
FROM sessions_device_readings
GROUP BY device_code, DATE_TRUNC('HOUR', reading_ts)
