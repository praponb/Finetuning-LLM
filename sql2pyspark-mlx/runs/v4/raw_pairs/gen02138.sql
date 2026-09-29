WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CONCAT_WS('/', firmware, device_code) firmware_device_code_key,
        FLOOR(humidity) humidity_whole
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT device_code, id
    FROM valid
    EXCEPT DISTINCT
    SELECT device_code, id
    FROM device_readings
    WHERE temp_c < 31
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.device_code = k.device_code AND s.id = k.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_device_code_key,
        humidity_whole,
        humidity / NULLIF(status_bits, 0) humidity_per_status_bits
    FROM verified
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 04:00:00')
      AND TO_DATE(reading_ts) >= '2025-04-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(humidity_whole) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) humidity_whole_sum_2h,
        PERCENT_RANK() OVER (ORDER BY pressure DESC NULLS FIRST) pressure_percent_rank,
        AVG(temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) temp_c_avg_1h
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(temp_c) OVER w_recent temp_c_max_15m,
        CUME_DIST() OVER w_grp pressure_cume_dist,
        MAX(humidity_per_status_bits) OVER w_recent humidity_per_status_bits_max_15m,
        SUM(temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) temp_c_sum_30m,
        PERCENT_RANK() OVER w_part pressure_percent_rank_2
    FROM metrics
    WINDOW w_recent AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY pressure DESC NULLS FIRST), w_part AS (ORDER BY pressure)
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY temp_c DESC NULLS FIRST) temp_c_dense_rank,
        PERCENT_RANK() OVER (ORDER BY humidity_whole DESC NULLS FIRST) humidity_whole_percent_rank,
        SUM(temp_c_sum_30m) OVER w_recent temp_c_sum_30m_sum_15m
    FROM scored
    WINDOW w_recent AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(pressure_percent_rank_2, pressure_percent_rank) least_pressure_percent_rank_2_pressure_percent_rank,
        ROUND(humidity_per_status_bits, 0) humidity_per_status_bits_rounded
    FROM metrics_device_readings
    WHERE humidity_whole_percent_rank BETWEEN 0.21 AND 0.76
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN humidity_per_status_bits >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_per_status_bits_tier,
    CASE WHEN temp_c_max_15m >= 38 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_max_15m_tier,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    PERCENTILE_APPROX(humidity_per_status_bits_rounded, ARRAY(0.25, 0.5, 0.75)) AS humidity_per_status_bits_rounded_quartiles,
    APPROX_PERCENTILE(pressure_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS pressure_percent_rank_2_quartiles,
    COUNT(*) AS row_count,
    BIT_OR(status_bits) AS status_bits_bit_or,
    MAX(reading_ts) AS ended_at
FROM cleaned
GROUP BY CUBE (DATE_TRUNC('HOUR', reading_ts), CASE WHEN humidity_per_status_bits >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN temp_c_max_15m >= 38 THEN 'PREMIUM' ELSE 'STANDARD' END);
