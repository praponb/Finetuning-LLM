WITH base AS (
    SELECT
        *,
        status_bits * pressure AS status_bits_times_pressure,
        CONCAT_WS('-', firmware, device_code) AS firmware_device_code_key
    FROM device_readings
    WHERE temp_c >= 25
),
scored AS (
    SELECT
        *,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_avg_1h,
        LEAD(pressure, 1) OVER (ORDER BY pressure, id) AS next_pressure,
        SUM(status_bits_times_pressure) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits_times_pressure
    FROM base
),
deduped AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_device_code_key,
        status_bits_avg_1h,
        next_pressure,
        cumulative_status_bits_times_pressure
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) = 1
),
metrics AS (
    SELECT
        *,
        MAX(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_temp_c,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_temp_c,
        MIN(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_temp_c,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY cumulative_status_bits_times_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure
    FROM deduped
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    status_bits_avg_1h,
    next_pressure,
    CASE WHEN device_code RLIKE '^(DV-04|DV-01)$' THEN 'CORE' ELSE 'GROUP_A' END AS device_code_zone
FROM metrics
ORDER BY device_code_zone DESC, temp_c
