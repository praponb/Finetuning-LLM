WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(status_bits * 1.1, 2) AS status_bits_adjusted
    FROM device_readings
    WHERE device_code = 'DV-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(firmware) AS clean_firmware,
        ROUND(temp_c / pressure, 2) AS temp_c_per_pressure
    FROM cleaned
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
),
cleaned_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        status_bits,
        status_bits_adjusted,
        clean_firmware,
        temp_c_per_pressure,
        LOWER(firmware) AS firmware_lower
    FROM filtered
    WHERE humidity IS NOT NULL
),
cleaned_device_readings_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_bits,
        status_bits_adjusted,
        clean_firmware,
        temp_c_per_pressure,
        GREATEST(status_bits, temp_c_per_pressure) AS greatest_status_bits_temp_c_per_pressure
    FROM cleaned_device_readings
    WHERE temp_c_per_pressure <= 55
)
-- Running and moving aggregates
SELECT
    id,
    clean_firmware,
    LEAD(greatest_status_bits_temp_c_per_pressure, 2) OVER (ORDER BY status_bits_adjusted, id) AS next_greatest_status_bits_temp_c_per_pressure,
    CUME_DIST() OVER (ORDER BY temp_c_per_pressure DESC NULLS FIRST) AS temp_c_per_pressure_cume_dist
FROM cleaned_device_readings_2
WHERE temp_c_per_pressure > 57
