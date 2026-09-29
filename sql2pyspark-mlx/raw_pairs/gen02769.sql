WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), reading_ts) / 365 AS years_since_reading_ts
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND device_code = 'DV-03'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(years_since_reading_ts) IGNORE NULLS OVER (PARTITION BY YEAR(reading_ts), device_code ORDER BY years_since_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_years_since_reading_ts,
        SUM(temp_c) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        COUNT(years_since_reading_ts) OVER (PARTITION BY YEAR(reading_ts) ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS years_since_reading_ts_count_2h,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('MONTH', reading_ts) ORDER BY reading_ts DESC, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY cumulative_temp_c DESC NULLS LAST, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS seq_no,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure
    FROM latest
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        years_since_reading_ts,
        last_known_years_since_reading_ts,
        cumulative_temp_c,
        years_since_reading_ts_count_2h,
        row_seq,
        row_num,
        seq_no,
        cumulative_pressure,
        CONCAT_WS(',', device_code, firmware) AS device_code_firmware_key
    FROM metrics_device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(seq_no / row_num, 2) AS seq_no_per_row_num
    FROM cleaned
    WHERE row_num <> 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_bits) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        NTILE(5) OVER w_hist AS years_since_reading_ts_count_2h_bucket,
        NTILE(3) OVER w_hist_2 AS seq_no_per_row_num_bucket
    FROM valid
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY years_since_reading_ts_count_2h, id), w_hist_2 AS (PARTITION BY DATE_TRUNC('MONTH', reading_ts), device_code ORDER BY seq_no_per_row_num, id)
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    status_bits,
    firmware,
    cumulative_temp_c,
    years_since_reading_ts_count_2h
FROM scored
ORDER BY firmware;
