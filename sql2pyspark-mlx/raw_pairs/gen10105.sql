WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    pressure,
    status_bits,
    firmware,
    burst_id
FROM sessions_device_readings
ORDER BY pressure DESC, reading_ts
