WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c - humidity AS temp_c_minus_humidity
    FROM device_readings
    WHERE device_code = 'DV-01'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT id, humidity
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(humidity) OVER (ORDER BY humidity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS humidity_avg_5_prior,
        AVG(humidity) OVER (ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS humidity_avg_2,
        NTILE(3) OVER (ORDER BY humidity, id) AS humidity_bucket
    FROM deduped
    WHERE humidity <= 54
),
filtered_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, humidity_avg_5_prior - humidity AS humidity_avg_5_prior_minus_humidity
    FROM enriched
    WHERE humidity_avg_2 <= 62
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS humidity_bucket_tier,
    COUNT(*) AS num_rows,
    SUM(humidity_avg_5_prior_minus_humidity) AS total_humidity_avg_5_prior_minus_humidity,
    MAX(humidity_bucket) AS max_humidity_bucket,
    APPROX_PERCENTILE(humidity, 0.5) AS humidity_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(humidity_avg_2, ARRAY(0.25, 0.5, 0.75)) AS humidity_avg_2_quartiles
FROM filtered_device_readings
GROUP BY CASE WHEN humidity_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END
