WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        CASE WHEN firmware IN ('1.0.3', '1.1.0') THEN 'Tier 1' ELSE 'Other' END firmware_group
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT device_code, id
FROM filtered
INTERSECT ALL
SELECT device_code, id
FROM device_readings
WHERE humidity > 45
