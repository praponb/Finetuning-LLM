WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 04:00:00'
      AND status_bits <= 11
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        pressure,
        status_bits,
        reading_ts_day,
        NTILE(3) OVER w_recent AS pressure_bucket,
        LEAD(pressure, 1) OVER w_recent AS next_pressure,
        AVG(status_bits) OVER w_ordered AS status_bits_avg_6,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS pressure_avg_6_prior
    FROM cleaned
    WHERE device_code IN ('DV-02', 'DV-04', 'DV-01')
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY pressure, id), w_ordered AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        reading_ts_day,
        pressure_bucket,
        next_pressure,
        status_bits_avg_6,
        CASE WHEN status_bits >= 12 THEN 'High' WHEN status_bits >= 3 THEN 'Medium' ELSE 'Low' END AS status_bits_band
    FROM enriched
    WHERE next_pressure <= 1010
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(pressure_bucket) OVER (ORDER BY next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_pressure_bucket,
        MIN(pressure_bucket) OVER (ORDER BY next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_pressure_bucket,
        MAX(pressure_bucket) OVER (ORDER BY next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_pressure_bucket
    FROM valid
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        reading_ts_day,
        pressure_bucket,
        next_pressure,
        status_bits_band,
        PERCENT_RANK() OVER w_running AS status_bits_avg_6_percent_rank,
        ROW_NUMBER() OVER (ORDER BY pressure_bucket, id) AS event_seq
    FROM metrics
    WINDOW w_running AS (ORDER BY status_bits_avg_6 DESC)
)
-- Window metrics over several different partitions
SELECT
    id,
    reading_ts,
    reading_ts_day,
    SUM(status_bits_avg_6_percent_rank) OVER (ORDER BY reading_ts_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_status_bits_avg_6_percent_rank,
    MIN(status_bits_avg_6_percent_rank) OVER (ORDER BY reading_ts_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_bits_avg_6_percent_rank,
    MAX(status_bits_avg_6_percent_rank) OVER (ORDER BY reading_ts_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_bits_avg_6_percent_rank,
    AVG(status_bits_avg_6_percent_rank) OVER (ORDER BY reading_ts_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_bits_avg_6_percent_rank
FROM enriched_device_readings
