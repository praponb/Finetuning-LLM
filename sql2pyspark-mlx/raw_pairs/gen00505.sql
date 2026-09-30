WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(device_code) AS device_code_upper,
        HOUR(reading_ts) AS reading_ts_hour
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 08:00:00'
)
SELECT device_code, firmware
FROM cleaned
INTERSECT
SELECT device_code, firmware
FROM device_readings
WHERE humidity < 47;
