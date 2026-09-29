WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        ROUND(pressure, 2) AS pressure_rounded,
        CEIL(pressure) AS pressure_whole
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.1.0', '1.0.3')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        pressure_rounded,
        pressure_whole,
        session_start_flag,
        island_id,
        AVG(session_start_flag) OVER (ORDER BY pressure_rounded ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS session_start_flag_avg_3,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        COUNT(session_start_flag) OVER (PARTITION BY device_code) AS device_code_count_session_start_flag
    FROM islands
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        pressure_rounded,
        pressure_whole,
        session_start_flag,
        island_id,
        session_start_flag_avg_3,
        humidity_share,
        FLOOR(humidity) AS humidity_whole,
        ROUND(pressure_whole * 1.1, 2) AS adjusted_pressure_whole
    FROM scored
    WHERE reading_ts >= TIMESTAMP '2025-09-01 16:00:00'
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM filtered_device_readings
),
islands_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY session_start_flag_avg_3 DESC NULLS FIRST) AS rank_in_group
        FROM islands_device_readings
    ) t
    WHERE rank_in_group = 1
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(pressure_rounded) OVER (PARTITION BY device_code ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pressure_rounded_avg_2,
    STDDEV_SAMP(island_id) OVER (PARTITION BY device_code) AS device_code_stddev_samp_island_id
FROM ranked
