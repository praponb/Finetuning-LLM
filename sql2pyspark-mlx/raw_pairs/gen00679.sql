WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    device_code,
    session_id,
    CASE WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'GROUP_A' ELSE 'CORE' END AS device_code_zone
FROM sessions_device_readings
ORDER BY device_code_zone;
