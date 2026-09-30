WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    firmware,
    is_new_burst,
    CASE WHEN device_code RLIKE '^(DV-04|DV-03)$' THEN 'PRIORITY' WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS device_code_zone
FROM sessions_device_readings;
