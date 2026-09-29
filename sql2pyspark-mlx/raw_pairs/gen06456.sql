WITH matched_keys AS (
    SELECT device_code, id
    FROM device_readings
    EXCEPT DISTINCT
    SELECT device_code, id
    FROM device_readings
    WHERE status_bits < 9
),
verified AS (
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.device_code = k.device_code AND s.id = k.id
)
SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware
FROM verified
ORDER BY status_bits ASC NULLS LAST
