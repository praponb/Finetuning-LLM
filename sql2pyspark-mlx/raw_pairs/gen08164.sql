WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c - pressure AS temp_c_minus_pressure
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        NTILE(4) OVER (ORDER BY status_bits DESC, id) AS status_bits_bucket,
        CORR(humidity, pressure) OVER (PARTITION BY YEAR(reading_ts)) AS corr_humidity_pressure
    FROM filtered
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        temp_c_minus_pressure,
        first_known_pressure,
        status_bits_bucket,
        corr_humidity_pressure,
        CEIL(pressure) AS pressure_whole,
        ROUND(temp_c_minus_pressure * 0.85, 2) AS temp_c_minus_pressure_adjusted
    FROM enriched
    WHERE firmware = '2.0.1'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        firmware,
        corr_humidity_pressure,
        pressure_whole,
        temp_c_minus_pressure_adjusted,
        SUM(status_bits_bucket) OVER w_running AS running_sum_status_bits_bucket,
        COUNT(status_bits_bucket) OVER w_running AS running_count_status_bits_bucket,
        MAX(status_bits_bucket) OVER w_running AS running_max_status_bits_bucket
    FROM valid
    WINDOW w_running AS (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(temp_c_minus_pressure_adjusted, 1) OVER (ORDER BY running_max_status_bits_bucket, id) AS next_temp_c_minus_pressure_adjusted,
        COUNT(running_sum_status_bits_bucket) OVER (PARTITION BY DATE_TRUNC('week', reading_ts) ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS running_sum_status_bits_bucket_count_2h
    FROM metrics
    WHERE firmware != '2.0.1'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN running_count_status_bits_bucket >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_status_bits_bucket_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    PERCENTILE_APPROX(corr_humidity_pressure, ARRAY(0.25, 0.5, 0.75)) AS corr_humidity_pressure_quartiles,
    APPROX_PERCENTILE(pressure_whole, ARRAY(0.25, 0.5, 0.75)) AS pressure_whole_quartiles,
    MIN(reading_ts) AS first_seen,
    PERCENTILE_APPROX(running_max_status_bits_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_max_status_bits_bucket_quartiles,
    PERCENTILE_APPROX(pressure, 0.9) AS pressure_p90,
    ROUND(AVG(running_count_status_bits_bucket), 2) AS avg_running_count_status_bits_bucket
FROM metrics_device_readings
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', reading_ts), CASE WHEN running_count_status_bits_bucket >= 13 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('DAY', reading_ts)), (DATE_TRUNC('HOUR', reading_ts), CASE WHEN running_count_status_bits_bucket >= 13 THEN 'LARGE' ELSE 'SMALL' END))
HAVING avg_running_count_status_bits_bucket >= 0
ORDER BY pressure_p90 DESC NULLS LAST;
