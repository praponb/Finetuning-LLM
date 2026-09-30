WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, YEAR(reading_ts) AS reading_ts_year
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        session_id,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS humidity_sum_2h,
        SUM(session_id) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_id
    FROM islands
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (ORDER BY session_id DESC, id ASC) AS session_id_bucket,
        AVG(humidity) OVER (ORDER BY cumulative_session_id ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS humidity_avg_5_prior,
        NTILE(10) OVER (ORDER BY session_id DESC, id ASC) AS session_id_bucket_2
    FROM metrics
    WHERE cumulative_session_id = 12
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, humidity / NULLIF(session_id, 0) AS humidity_per_session_id
    FROM metrics_device_readings
    WHERE session_id_bucket_2 <= 5
)
-- Grouped metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    PERCENTILE_APPROX(session_id_bucket, 0.5) AS session_id_bucket_p50,
    ROUND(AVG(humidity_sum_2h), 2) AS avg_humidity_sum_2h,
    AVG(humidity_per_session_id) AS avg_humidity_per_session_id,
    ROUND(AVG(cumulative_session_id), 2) AS avg_cumulative_session_id,
    APPROX_PERCENTILE(session_id_bucket, ARRAY(0.25, 0.5, 0.75)) AS session_id_bucket_quartiles
FROM filtered
GROUP BY 1
HAVING PERCENTILE_APPROX(session_id_bucket, 0.5) > 0
ORDER BY avg_humidity_sum_2h ASC
