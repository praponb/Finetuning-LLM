WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        ROUND(pressure / temp_c, 2) pressure_per_temp_c
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 05:00:00'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) seq_no
    FROM sessionized
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        new_group_flag,
        session_no,
        row_seq,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_pressure,
        CORR(temp_c, status_bits) OVER (PARTITION BY MONTH(reading_ts), device_code) corr_temp_c_status_bits,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_row_seq
    FROM metrics
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        pressure,
        new_group_flag,
        session_no,
        row_seq,
        corr_temp_c_status_bits,
        SUM(first_known_pressure) OVER (ORDER BY corr_temp_c_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_first_known_pressure,
        NTILE(10) OVER (ORDER BY temp_c, id) temp_c_bucket,
        LAST_VALUE(new_group_flag) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('week', reading_ts) ORDER BY last_known_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_new_group_flag,
        SUM(session_no) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) session_no_sum_1h,
        CORR(pressure, temp_c) OVER (PARTITION BY MONTH(reading_ts), device_code) corr_pressure_temp_c
    FROM with_windows
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(last_known_new_group_flag, 1) OVER (ORDER BY running_first_known_pressure, id) next_last_known_new_group_flag,
        RANK() OVER (ORDER BY session_no_sum_1h) session_no_sum_1h_rank
    FROM with_windows_device_readings
),
cleaned_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        pressure,
        row_seq,
        corr_temp_c_status_bits,
        running_first_known_pressure,
        temp_c_bucket,
        last_known_new_group_flag,
        session_no_sum_1h,
        corr_pressure_temp_c,
        next_last_known_new_group_flag,
        GREATEST(pressure, new_group_flag) greatest_pressure_new_group_flag
    FROM metrics_device_readings
    WHERE next_last_known_new_group_flag IS NOT NULL
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN corr_temp_c_status_bits >= 0.41 THEN 'HIGH' ELSE 'LOW' END AS corr_temp_c_status_bits_tier,
        CASE WHEN next_last_known_new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_last_known_new_group_flag_tier,
        PERCENTILE_APPROX(temp_c_bucket, ARRAY(0.25, 0.5, 0.75)) AS temp_c_bucket_quartiles,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned_device_readings
    GROUP BY corr_temp_c_status_bits_tier, next_last_known_new_group_flag_tier
)
SELECT
    corr_temp_c_status_bits_tier,
    next_last_known_new_group_flag_tier,
    distinct_ids
FROM totals
ORDER BY corr_temp_c_status_bits_tier DESC NULLS FIRST;
