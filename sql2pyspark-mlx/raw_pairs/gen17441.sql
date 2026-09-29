WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        humidity,
        status_bits,
        SUM(pressure) OVER w_running AS running_pressure,
        LEAD(temp_c, 1) OVER (ORDER BY humidity, id) AS next_temp_c,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_max_15m,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_sum_1h,
        LAG(humidity, 2) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_humidity
    FROM device_readings
    WINDOW w_running AS (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(humidity, 0) AS humidity_filled,
        running_pressure / NULLIF(status_bits_max_15m, 0) AS running_pressure_per_status_bits_max_15m
    FROM metrics
    WHERE reading_ts >= TIMESTAMP '2025-09-01 08:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(prev_humidity) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_humidity_avg_3_prior,
        AVG(status_bits_max_15m) OVER (ORDER BY humidity_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_bits_max_15m_avg_3,
        SUM(prev_humidity) OVER (ORDER BY temp_c_sum_1h, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_humidity,
        AVG(status_bits) OVER (ORDER BY status_bits_max_15m, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        NTILE(5) OVER (ORDER BY temp_c_sum_1h DESC, id) AS temp_c_sum_1h_bucket
    FROM cleaned
    WHERE humidity BETWEEN 54 AND 75
),
cleaned_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        humidity,
        status_bits,
        running_pressure,
        next_temp_c,
        temp_c_sum_1h,
        humidity_filled,
        status_bits_max_15m_avg_3,
        running_prev_humidity,
        status_bits_avg_6_prior,
        COALESCE(running_pressure_per_status_bits_max_15m, 0) AS running_pressure_per_status_bits_max_15m_filled,
        status_bits_max_15m * temp_c_sum_1h AS status_bits_max_15m_times_temp_c_sum_1h
    FROM scored
    WHERE reading_ts > DATE '2025-09-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        humidity,
        status_bits,
        running_pressure,
        next_temp_c,
        temp_c_sum_1h,
        running_prev_humidity,
        status_bits_avg_6_prior,
        running_pressure_per_status_bits_max_15m_filled,
        status_bits_max_15m_times_temp_c_sum_1h,
        next_temp_c * running_pressure AS next_temp_c_times_running_pressure
    FROM cleaned_device_readings
    WHERE status_bits_max_15m_avg_3 < 3
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY status_bits_avg_6_prior DESC, id) AS event_seq,
        RANK() OVER (ORDER BY humidity NULLS LAST) AS humidity_rank,
        LEAD(status_bits_avg_6_prior, 1) OVER (ORDER BY next_temp_c_times_running_pressure, id) AS next_status_bits_avg_6_prior
    FROM filtered
)
SELECT
    id,
    humidity,
    running_pressure,
    next_temp_c,
    running_prev_humidity,
    status_bits_avg_6_prior,
    next_temp_c_times_running_pressure,
    event_seq,
    humidity_rank
FROM metrics_device_readings
ORDER BY running_prev_humidity;
