WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CONCAT(device_code, ' ', firmware) AS device_code_firmware
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2025-02-08'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        firmware,
        ROUND(status_bits, 2) AS status_bits_rounded
    FROM base
    WHERE temp_c BETWEEN 24 AND 36
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits_rounded,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_humidity,
        MAX(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_humidity,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_humidity,
        LAST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY running_max_humidity DESC) AS running_max_humidity_dense_rank,
        SUM(temp_c) OVER (ORDER BY running_max_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        MAX(humidity) OVER (PARTITION BY device_code) AS device_code_max_humidity
    FROM scored
),
cleaned_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(running_temp_c AS STRING) AS running_temp_c_str,
        humidity - running_temp_c AS humidity_minus_running_temp_c
    FROM enriched
    WHERE temp_c <= 22
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        status_bits_rounded,
        running_sum_humidity,
        running_max_humidity_dense_rank,
        MIN(running_min_humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_running_min_humidity,
        AVG(running_min_humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_running_min_humidity,
        SUM(running_min_humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_running_min_humidity
    FROM cleaned_device_readings
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_max_humidity_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS running_max_humidity_dense_rank_tier,
        CASE WHEN last3_sum_running_min_humidity >= 300 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_sum_running_min_humidity_tier,
        CASE WHEN temp_c >= 40.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
        APPROX_PERCENTILE(last3_sum_running_min_humidity, 0.95) AS last3_sum_running_min_humidity_p95,
        COUNT(*) AS num_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        PERCENTILE_APPROX(last3_min_running_min_humidity, 0.9) AS last3_min_running_min_humidity_p90,
        GROUPING(CASE WHEN running_max_humidity_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM with_windows
    GROUP BY CUBE (CASE WHEN running_max_humidity_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last3_sum_running_min_humidity >= 300 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN temp_c >= 40.9 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING distinct_ids_2 >= 1
)
SELECT
    running_max_humidity_dense_rank_tier,
    num_rows,
    distinct_ids,
    distinct_ids_2,
    last3_min_running_min_humidity_p90,
    gid
FROM totals
ORDER BY gid DESC
