WITH base AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        INITCAP(TRIM(device_code)) device_code_title
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND device_code IN ('DV-04', 'DV-03')
),
filtered AS (
    SELECT
        *,
        HOUR(reading_ts) reading_ts_hour,
        TRIM(firmware) clean_firmware
    FROM base
    WHERE temp_c BETWEEN 22 AND 27
),
valid AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        device_code_title,
        CEIL(pressure) pressure_whole
    FROM filtered
    WHERE temp_c >= 38
),
base_device_readings AS (
    SELECT *, TO_DATE(reading_ts) reading_ts_date
    FROM valid
    WHERE reading_ts >= DATE '2025-09-01'
      AND status_bits BETWEEN 8 AND 9
),
enriched AS (
    SELECT
        id,
        reading_ts,
        firmware,
        pressure_whole,
        reading_ts_date,
        LEAD(pressure_whole) OVER (ORDER BY status_bits, id) next_pressure_whole,
        LEAD(temp_c) OVER (ORDER BY status_bits, id) next_temp_c,
        ROW_NUMBER() OVER (ORDER BY status_bits, id) event_seq,
        SUM(status_bits) OVER (PARTITION BY DATE_TRUNC('month', reading_ts_date) ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_status_bits
    FROM base_device_readings
    WHERE pressure_whole > 41
),
enriched_device_readings AS (
    SELECT
        *,
        COUNT(next_temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) next_temp_c_count_2h,
        DENSE_RANK() OVER (ORDER BY next_pressure_whole NULLS LAST) next_pressure_whole_dense_rank,
        NTILE(5) OVER (ORDER BY next_pressure_whole DESC, id) next_pressure_whole_bucket,
        LAG(next_temp_c, 2) OVER (ORDER BY next_pressure_whole, id) prev_next_temp_c
    FROM enriched
),
metrics AS (
    SELECT
        id,
        pressure_whole,
        reading_ts_date,
        next_pressure_whole,
        next_temp_c,
        running_status_bits,
        next_pressure_whole_dense_rank,
        LAG(next_pressure_whole_dense_rank) OVER w_ordered prev_next_pressure_whole_dense_rank,
        LAG(event_seq) OVER w_ordered prev_event_seq,
        ROW_NUMBER() OVER w_ordered event_seq_2,
        DENSE_RANK() OVER w_hist pressure_whole_dense_rank
    FROM enriched_device_readings
    WINDOW w_ordered AS (ORDER BY pressure_whole, id), w_hist AS (PARTITION BY YEAR(reading_ts) ORDER BY pressure_whole DESC NULLS FIRST)
)
SELECT
    id,
    pressure_whole,
    reading_ts_date,
    next_pressure_whole,
    next_temp_c,
    running_status_bits,
    next_pressure_whole_dense_rank,
    prev_next_pressure_whole_dense_rank,
    prev_event_seq
FROM metrics
ORDER BY next_temp_c, running_status_bits
