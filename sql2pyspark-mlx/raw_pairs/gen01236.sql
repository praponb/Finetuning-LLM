WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_humidity,
        MAX(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_humidity,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_humidity
    FROM device_readings
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        running_min_humidity,
        running_max_humidity,
        SUM(running_avg_humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS running_avg_humidity_sum_15m,
        AVG(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_avg_1h
    FROM enriched
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY status_bits_avg_1h DESC NULLS LAST) AS status_bits_avg_1h_rank,
        SUM(running_max_humidity) OVER (ORDER BY status_bits_avg_1h, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_humidity,
        AVG(running_max_humidity) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_max_humidity_avg_6_prior
    FROM metrics
    WHERE pressure < 1022
),
base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(firmware) AS firmware_upper
    FROM enriched_device_readings
    WHERE firmware = '1.1.0'
)
-- Ranking and gap features
SELECT
    id,
    reading_ts,
    pressure,
    firmware,
    running_min_humidity,
    running_max_humidity,
    running_avg_humidity_sum_15m,
    status_bits_avg_1h,
    running_running_max_humidity,
    running_max_humidity_avg_6_prior,
    firmware_upper,
    MAX(status_bits_avg_1h_rank) OVER w_recent AS running_max_status_bits_avg_1h_rank,
    AVG(status_bits_avg_1h_rank) OVER w_recent AS running_avg_status_bits_avg_1h_rank,
    SUM(status_bits_avg_1h_rank) OVER w_recent AS running_sum_status_bits_avg_1h_rank
FROM base
WINDOW w_recent AS (ORDER BY running_running_max_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
