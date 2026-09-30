WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN firmware = '1.0.3' THEN 1 ELSE 0 END AS is_1_0_3
    FROM device_readings
    WHERE firmware NOT IN ('2.0.1', '1.0.3')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_1_0_3,
        LAG(humidity) OVER w_part AS prev_humidity,
        LAG(temp_c) OVER w_part AS prev_temp_c,
        LEAD(temp_c) OVER w_part AS next_temp_c,
        ROW_NUMBER() OVER w_part AS seq_no,
        AVG(humidity) OVER (PARTITION BY MONTH(reading_ts), device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS humidity_avg_3_prior
    FROM base
    WINDOW w_part AS (PARTITION BY device_code ORDER BY temp_c, id)
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_humidity,
        next_temp_c,
        seq_no,
        humidity_avg_3_prior,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS status_bits_bucket,
        FIRST_VALUE(humidity_avg_3_prior) IGNORE NULLS OVER (ORDER BY prev_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity_avg_3_prior
    FROM metrics
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        next_temp_c,
        seq_no,
        humidity_avg_3_prior,
        first_known_humidity_avg_3_prior,
        humidity_avg_3_prior * status_bits_bucket AS humidity_avg_3_prior_times_status_bits_bucket,
        first_known_humidity_avg_3_prior - humidity AS first_known_humidity_avg_3_prior_minus_humidity
    FROM metrics_device_readings
    WHERE seq_no <= 6
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY first_known_humidity_avg_3_prior_minus_humidity NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    device_code,
    humidity,
    next_temp_c,
    seq_no,
    humidity_avg_3_prior,
    first_known_humidity_avg_3_prior,
    humidity_avg_3_prior_times_status_bits_bucket,
    first_known_humidity_avg_3_prior_minus_humidity,
    CASE WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'PRIORITY' ELSE 'CORE' END AS device_code_zone
FROM latest
ORDER BY first_known_humidity_avg_3_prior, first_known_humidity_avg_3_prior_minus_humidity DESC
