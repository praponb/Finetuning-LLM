WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(temp_c, humidity) OVER (PARTITION BY device_code) AS corr_temp_c_humidity,
        LAST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_count_1h
    FROM device_readings
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        corr_temp_c_humidity,
        last_known_pressure,
        temp_c_count_1h,
        AVG(last_known_pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last_known_pressure_avg_3_prior,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS temp_c_bucket
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        corr_temp_c_humidity,
        last_known_pressure,
        temp_c_count_1h,
        temp_c_bucket,
        ROUND(last_known_pressure / temp_c_count_1h, 2) AS last_known_pressure_per_temp_c_count_1h
    FROM metrics
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(pressure * 100, 2) AS adjusted_pressure
    FROM valid
    WHERE last_known_pressure >= 994
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        corr_temp_c_humidity,
        last_known_pressure,
        temp_c_count_1h,
        temp_c_bucket,
        last_known_pressure_per_temp_c_count_1h,
        adjusted_pressure,
        STDDEV_SAMP(pressure) OVER w_ordered AS device_code_stddev_samp_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY last_known_pressure, id) AS seq_no,
        LAST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure_2,
        SUM(temp_c_bucket) OVER (PARTITION BY device_code ORDER BY temp_c_count_1h, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c_bucket
    FROM base
    WINDOW w_ordered AS (PARTITION BY device_code)
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(temp_c) OVER w_running AS device_code_sum_temp_c,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY seq_no DESC, id) AS seq_no_bucket,
        LAST_VALUE(device_code_stddev_samp_pressure, TRUE) OVER w_part AS last_known_device_code_stddev_samp_pressure,
        AVG(corr_temp_c_humidity) OVER (PARTITION BY device_code ORDER BY last_known_pressure_per_temp_c_count_1h, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS corr_temp_c_humidity_avg_3,
        COUNT(last_known_pressure_2) OVER w_running AS device_code_count_last_known_pressure_2
    FROM scored
    WINDOW w_running AS (PARTITION BY device_code), w_part AS (PARTITION BY device_code ORDER BY adjusted_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        pressure,
        temp_c_count_1h,
        temp_c_bucket,
        last_known_pressure_per_temp_c_count_1h,
        device_code_stddev_samp_pressure,
        seq_no,
        last_known_pressure_2,
        cumulative_temp_c_bucket,
        seq_no_bucket,
        last_known_device_code_stddev_samp_pressure,
        corr_temp_c_humidity_avg_3,
        device_code_count_last_known_pressure_2,
        COALESCE(cumulative_temp_c_bucket, 0) AS cumulative_temp_c_bucket_filled
    FROM metrics_device_readings
    WHERE pressure >= 1026
)
-- Grouped metrics
SELECT
    seq_no,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    PERCENTILE_APPROX(seq_no_bucket, ARRAY(0.25, 0.5, 0.75)) AS seq_no_bucket_quartiles,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes
FROM filtered
GROUP BY 1
ORDER BY seq_no NULLS FIRST
