WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(reading_ts, 'yyyy-MM') AS reading_ts_label
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT device_code
    FROM base
    EXCEPT
    SELECT device_code
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 8 AND 16
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.device_code = k.device_code
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(status_bits) AS status_bits_whole
    FROM verified
    WHERE pressure > 1004
),
enriched AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY status_bits) AS status_bits_percent_rank
    FROM cleaned
    WHERE humidity IS NULL
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        status_bits,
        firmware,
        status_bits_whole,
        status_bits_percent_rank,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY humidity NULLS FIRST) AS humidity_dense_rank,
        STDDEV_SAMP(temp_c) OVER (PARTITION BY device_code) AS device_code_stddev_samp_temp_c
    FROM enriched
),
cleaned_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN firmware = '1.1.0' THEN 1 ELSE 0 END AS is_1_1_0
    FROM enriched_device_readings
    WHERE firmware = '2.0.1'
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_dense_rank_tier,
    PERCENTILE_APPROX(device_code_stddev_samp_temp_c, ARRAY(0.25, 0.5, 0.75)) AS device_code_stddev_samp_temp_c_quartiles,
    COUNT(*) AS cnt
FROM cleaned_device_readings
GROUP BY 1
