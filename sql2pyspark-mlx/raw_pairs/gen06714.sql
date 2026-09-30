WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c + humidity temp_c_plus_humidity
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND pressure BETWEEN 1002 AND 1020
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        status_bits,
        firmware
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_plus_humidity DESC NULLS LAST, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_bits * 1.1, 2) status_bits_adjusted
    FROM deduped
    WHERE firmware IN ('1.1.0', '1.0.3')
),
filtered_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(status_bits_adjusted, status_bits) greatest_status_bits_adjusted_status_bits
    FROM filtered
    WHERE status_bits_adjusted > 50
),
base_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        greatest_status_bits_adjusted_status_bits + status_bits greatest_status_bits_adjusted_status_bits_plus_status_bits,
        CEIL(status_bits) status_bits_whole
    FROM filtered_device_readings
    WHERE status_bits_adjusted >= 33
)
SELECT
    id,
    firmware,
    status_bits_adjusted,
    greatest_status_bits_adjusted_status_bits,
    greatest_status_bits_adjusted_status_bits_plus_status_bits,
    status_bits_whole
FROM base_device_readings
ORDER BY status_bits_adjusted DESC NULLS FIRST;
