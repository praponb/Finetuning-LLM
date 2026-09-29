WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM islands
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY island_id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(is_new_burst, TRUE) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_burst,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share
    FROM latest
    WHERE island_id <= 5
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) IS NULL
                 OR prev_reading_ts > LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands_device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        is_new_burst,
        first_known_is_new_burst,
        prev_reading_ts,
        is_new_session,
        session_id,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            firmware,
            device_code,
            is_new_session
        FROM sessions_device_readings
    ) src
    PIVOT (
        SUM(is_new_session)
        FOR device_code IN ('DV-02' AS dv_02, 'DV-01' AS dv_01)
    )
)
SELECT firmware, dv_01
FROM wide
ORDER BY dv_01, firmware
