WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share,
        RANK() OVER (ORDER BY pressure ASC NULLS FIRST) AS pressure_rank,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS row_seq,
        AVG(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_avg_2h
    FROM device_readings
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM sessions_device_readings
),
gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_reading_ts ASC NULLS FIRST, id ASC) = 1
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            firmware,
            device_code,
            pressure_avg_2h
        FROM ranked
    ) src
    PIVOT (
        AVG(pressure_avg_2h) AS amount,
        COUNT(1) AS cnt
        FOR device_code IN ('DV-02' AS dv_02, 'DV-03' AS dv_03, 'DV-01' AS dv_01)
    )
)
SELECT
    firmware,
    dv_02_amount,
    dv_02_cnt,
    dv_03_cnt,
    dv_01_amount,
    dv_01_cnt
FROM by_type
ORDER BY dv_01_amount ASC
