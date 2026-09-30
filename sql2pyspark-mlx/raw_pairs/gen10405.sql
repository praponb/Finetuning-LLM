WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS LAST, id) = 1
)
SELECT
    id,
    temp_c,
    pressure,
    status_bits
FROM ranked
ORDER BY status_bits DESC NULLS FIRST, id;
