WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_sum_30m,
        COUNT(humidity) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_count_1h
    FROM device_readings
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        LAG(humidity_count_1h, 1) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_humidity_count_1h,
        NTILE(5) OVER (ORDER BY humidity_count_1h, id) AS humidity_count_1h_bucket,
        LAST_VALUE(humidity_sum_30m, TRUE) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity_sum_30m
    FROM with_windows
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(prev_humidity_count_1h) OVER w_grp AS prev_prev_humidity_count_1h,
        LEAD(prev_humidity_count_1h) OVER w_grp AS next_prev_humidity_count_1h,
        LEAD(humidity_count_1h_bucket) OVER w_grp AS next_humidity_count_1h_bucket,
        ROW_NUMBER() OVER w_grp AS row_seq,
        COUNT(temp_c) OVER w_ordered AS temp_c_count_15m
    FROM metrics
    WINDOW w_grp AS (ORDER BY last_known_humidity_sum_30m, id), w_ordered AS (PARTITION BY MONTH(reading_ts) ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        prev_humidity_count_1h,
        humidity_count_1h_bucket,
        last_known_humidity_sum_30m,
        prev_prev_humidity_count_1h,
        row_seq,
        humidity_count_1h_bucket * temp_c AS humidity_count_1h_bucket_times_temp_c,
        CASE WHEN next_humidity_count_1h_bucket >= 8 THEN 'High' WHEN next_humidity_count_1h_bucket >= 3 THEN 'Medium' ELSE 'Low' END AS next_humidity_count_1h_bucket_level
    FROM enriched
    WHERE last_known_humidity_sum_30m >= 298
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        temp_c,
        firmware,
        prev_humidity_count_1h,
        humidity_count_1h_bucket,
        last_known_humidity_sum_30m,
        prev_prev_humidity_count_1h,
        row_seq,
        humidity_count_1h_bucket_times_temp_c,
        next_humidity_count_1h_bucket_level,
        COALESCE(humidity_count_1h_bucket, 0) AS humidity_count_1h_bucket_filled,
        CASE WHEN last_known_humidity_sum_30m >= 395 THEN 'High' WHEN last_known_humidity_sum_30m >= 149 THEN 'Medium' ELSE 'Low' END AS last_known_humidity_sum_30m_level
    FROM base
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
      AND row_seq > 6
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        firmware,
        prev_humidity_count_1h,
        last_known_humidity_sum_30m,
        row_seq,
        humidity_count_1h_bucket_times_temp_c,
        next_humidity_count_1h_bucket_level,
        humidity_count_1h_bucket_filled,
        last_known_humidity_sum_30m_level,
        CASE WHEN row_seq >= 7 THEN 'High' WHEN row_seq >= 3 THEN 'Medium' ELSE 'Low' END AS row_seq_level
    FROM cleaned
    WHERE humidity_count_1h_bucket_times_temp_c < 54
)
-- Ranking and gap features
SELECT
    *,
    MIN(humidity_count_1h_bucket_filled) OVER w_hist AS running_min_humidity_count_1h_bucket_filled,
    AVG(humidity_count_1h_bucket_filled) OVER w_hist AS running_avg_humidity_count_1h_bucket_filled,
    SUM(humidity_count_1h_bucket_filled) OVER w_hist AS running_sum_humidity_count_1h_bucket_filled,
    SUM(humidity_count_1h_bucket_filled) OVER w_hist AS running_humidity_count_1h_bucket_filled
FROM base_device_readings
WINDOW w_hist AS (ORDER BY last_known_humidity_sum_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
