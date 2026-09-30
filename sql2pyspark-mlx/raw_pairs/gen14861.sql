WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(status_bits AS DOUBLE) AS status_bits_num
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        firmware,
        status_bits_num,
        MIN(status_bits) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_status_bits,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_status_bits,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_status_bits
    FROM deduped
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(temp_c) OVER w_part AS prev_temp_c,
        LEAD(temp_c) OVER w_part AS next_temp_c,
        LEAD(last3_max_status_bits) OVER w_part AS next_last3_max_status_bits,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM metrics
    WINDOW w_part AS (ORDER BY status_bits_num ASC, id ASC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(next_temp_c * 1.15, 2) AS adjusted_next_temp_c
    FROM metrics_device_readings
    WHERE firmware = '2.0.1'
)
-- Ranking and gap features
SELECT
    id,
    firmware,
    status_bits_num,
    next_temp_c,
    next_last3_max_status_bits,
    adjusted_next_temp_c,
    MIN(temp_c) OVER w_hist AS last3_min_temp_c,
    MAX(temp_c) OVER w_hist AS last3_max_temp_c,
    COUNT(temp_c) OVER w_hist AS last3_count_temp_c,
    ROW_NUMBER() OVER (ORDER BY next_temp_c DESC, id ASC) AS row_seq
FROM base
WINDOW w_hist AS (ORDER BY next_last3_max_status_bits ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
