WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        session_start_flag,
        burst_id,
        MIN(status_bits) OVER (PARTITION BY device_code) AS device_code_min_status_bits,
        MAX(session_start_flag) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS session_start_flag_max_30m,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_avg_6,
        SUM(pressure) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
        MAX(burst_id) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS burst_id_max_30m
    FROM sessionized
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            firmware,
            device_code,
            temp_c
        FROM islands
    ) src
    PIVOT (
        AVG(temp_c) AS amount
        FOR device_code IN ('DV-03' AS dv_03, 'DV-02' AS dv_02)
    )
)
SELECT
    firmware,
    dv_03,
    dv_02
FROM by_type
ORDER BY dv_02 DESC, firmware
