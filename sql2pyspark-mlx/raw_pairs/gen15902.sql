WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits,
        device_code_upper,
        LEAD(temp_c, 1) OVER (ORDER BY humidity, id) AS next_temp_c,
        SUM(status_bits) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits,
        NTILE(4) OVER (ORDER BY temp_c DESC, id) AS temp_c_bucket
    FROM filtered
),
filtered_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        next_temp_c,
        cumulative_status_bits,
        temp_c / NULLIF(status_bits, 0) AS temp_c_per_status_bits
    FROM metrics
    WHERE cumulative_status_bits BETWEEN 33 AND 38
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        cumulative_status_bits,
        temp_c_per_status_bits,
        ROUND(cumulative_status_bits * 1.07, 2) AS adjusted_cumulative_status_bits,
        ROUND(cumulative_status_bits * 100, 2) AS cumulative_status_bits_scaled
    FROM filtered_device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
      AND cumulative_status_bits = 16
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cumulative_status_bits_scaled * temp_c_per_status_bits AS cumulative_status_bits_scaled_times_temp_c_per_status_bits,
        COALESCE(cumulative_status_bits_scaled, 0) AS cumulative_status_bits_scaled_filled
    FROM valid
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 02:00:00')
      AND adjusted_cumulative_status_bits >= 29
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), reading_ts) / 7 AS weeks_since_reading_ts,
        CAST(reading_ts AS DATE) AS reading_ts_day
    FROM base
    WHERE reading_ts >= DATE '2025-09-01'
      AND cumulative_status_bits != 59
),
valid_device_readings_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        cumulative_status_bits_scaled,
        cumulative_status_bits_scaled_times_temp_c_per_status_bits,
        cumulative_status_bits_scaled_filled,
        weeks_since_reading_ts,
        reading_ts_day,
        DATE_TRUNC('year', reading_ts_day) AS reading_ts_day_period,
        cumulative_status_bits + temp_c AS cumulative_status_bits_plus_temp_c
    FROM valid_device_readings
    WHERE reading_ts_day < '2024-11-23'
)
-- Running and moving aggregates
SELECT
    id,
    cumulative_status_bits_scaled_times_temp_c_per_status_bits,
    reading_ts_day,
    cumulative_status_bits_plus_temp_c,
    AVG(temp_c) OVER w_ordered AS running_avg_temp_c,
    SUM(temp_c) OVER w_ordered AS running_sum_temp_c,
    COUNT(temp_c) OVER w_ordered AS running_count_temp_c,
    MIN(temp_c) OVER w_ordered AS running_min_temp_c,
    RANK() OVER w_part AS cumulative_status_bits_scaled_rank
FROM valid_device_readings_2
WHERE reading_ts_day_period >= '2024-10-01'
WINDOW w_ordered AS (ORDER BY cumulative_status_bits_scaled_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY cumulative_status_bits_scaled NULLS FIRST)
