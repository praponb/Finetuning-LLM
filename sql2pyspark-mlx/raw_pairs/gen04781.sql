WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(temp_c AS INT) AS temp_c_int
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.1.0', '1.0.3')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM sessionized
),
gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(firmware, 1, 3) AS firmware_prefix
    FROM sessionized_device_readings
    WHERE burst_id > 3
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        prev_reading_ts,
        is_new_session,
        burst_id,
        CAST(prev_reading_ts AS DATE) AS prev_reading_ts_day
    FROM valid
    WHERE device_code = 'DV-01'
)
SELECT
    id,
    device_code,
    temp_c,
    firmware,
    prev_reading_ts,
    burst_id,
    prev_reading_ts_day
FROM base_device_readings
ORDER BY firmware NULLS FIRST, prev_reading_ts
