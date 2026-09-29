WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        GREATEST(pressure, temp_c) AS greatest_pressure_temp_c
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        humidity,
        firmware,
        session_no,
        CASE WHEN device_code IN ('DV-03', 'DV-04') THEN 'Group A' ELSE 'Other' END AS device_code_group,
        humidity - session_no AS humidity_minus_session_no
    FROM islands
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY session_no DESC, id) AS session_no_bucket,
        AVG(humidity_minus_session_no) OVER (ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS humidity_minus_session_no_avg_3_prior
    FROM base_device_readings
    WHERE humidity_minus_session_no < 50
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        humidity,
        firmware,
        device_code_group,
        humidity_minus_session_no,
        humidity_minus_session_no_avg_3_prior,
        COALESCE(session_no_bucket, 0) AS session_no_bucket_filled,
        CASE WHEN session_no >= 9 THEN 'High' WHEN session_no >= 3 THEN 'Medium' ELSE 'Low' END AS session_no_level
    FROM metrics
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 05:00:00')
)
SELECT
    id,
    reading_ts,
    humidity,
    device_code_group,
    humidity_minus_session_no_avg_3_prior,
    session_no_level
FROM cleaned;
