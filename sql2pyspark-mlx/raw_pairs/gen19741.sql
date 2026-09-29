WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, temp_c + status_bits AS temp_c_plus_status_bits
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND TO_DATE(reading_ts) < '2023-11-01'
),
filtered_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        temp_c_plus_status_bits,
        ROUND(humidity, 0) AS humidity_rounded
    FROM filtered
    WHERE reading_ts > DATE '2025-09-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        LAG(temp_c_plus_status_bits) OVER (PARTITION BY device_code ORDER BY humidity_rounded ASC, id ASC) AS prev_temp_c_plus_status_bits,
        LAG(temp_c) OVER (PARTITION BY device_code ORDER BY humidity_rounded ASC, id ASC) AS prev_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_rounded ASC, id ASC) AS event_seq
    FROM filtered_device_readings
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN session_start_flag_2 >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_2_tier,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    MIN(reading_ts) AS first_reading_ts,
    APPROX_PERCENTILE(temp_c, 0.5) AS temp_c_p50,
    SUM(CASE WHEN device_code = 'DV-02' THEN session_start_flag ELSE 0 END) AS dv_02_session_start_flag,
    ROUND(AVG(prev_temp_c), 2) AS avg_prev_temp_c
FROM islands
GROUP BY activity_day, session_start_flag_2_tier
HAVING num_device_codes < 1000
