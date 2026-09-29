WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(firmware, 1, 4) AS firmware_prefix
    FROM device_readings
    WHERE pressure >= 1003
)
-- Keep one row per key (latest first)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    status_bits,
    firmware,
    firmware_prefix
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS rn
    FROM cleaned
) ranked
WHERE rn = 1
