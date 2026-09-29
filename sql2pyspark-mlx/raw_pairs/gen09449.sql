WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    burst_id,
    CASE WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'GROUP_A' WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS device_code_zone
FROM islands
ORDER BY reading_ts DESC NULLS LAST, device_code_zone DESC;
