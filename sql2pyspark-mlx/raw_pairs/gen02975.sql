WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        CAST(reading_ts AS DATE) AS reading_ts_day,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        reading_ts_day,
        NTILE(3) OVER w_part AS temp_c_bucket,
        DENSE_RANK() OVER (ORDER BY temp_c NULLS FIRST) AS temp_c_dense_rank
    FROM filtered
    WINDOW w_part AS (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY temp_c, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        device_code,
        pressure,
        temp_c_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_dense_rank DESC, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        pressure,
        LEAD(temp_c_bucket) OVER w_running AS next_temp_c_bucket,
        LAG(temp_c_bucket) OVER w_running AS prev_temp_c_bucket,
        LAG(pressure) OVER w_running AS prev_pressure,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS seq_no_2
    FROM latest
    WINDOW w_running AS (PARTITION BY device_code ORDER BY pressure, id)
)
SELECT
    id,
    pressure,
    next_temp_c_bucket,
    prev_pressure
FROM enriched
ORDER BY prev_pressure DESC
