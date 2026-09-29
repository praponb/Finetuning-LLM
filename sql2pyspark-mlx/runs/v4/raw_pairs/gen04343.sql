WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(status_bits / temp_c, 2) AS status_bits_per_temp_c,
        CAST(status_bits AS STRING) AS status_bits_str
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND pressure <= 1015
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM cleaned
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_reading_ts,
        is_new_session,
        session_id,
        HOUR(prev_reading_ts) AS prev_reading_ts_hour,
        session_id * is_new_session AS session_id_times_is_new_session
    FROM sessionized
    WHERE is_new_session != 0
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        temp_c,
        humidity
    FROM base
) src
UNPIVOT (
    reading FOR metric IN (pressure, temp_c, humidity)
);
