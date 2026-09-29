WITH matched_keys AS (
    -- Set difference on keys
    SELECT device_code
    FROM device_readings
    EXCEPT
    SELECT device_code
    FROM device_readings
    WHERE device_code NOT IN ('DV-03', 'DV-01')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.device_code = k.device_code
)
SELECT
    id,
    device_code,
    humidity,
    pressure,
    status_bits,
    firmware
FROM reconciled
ORDER BY pressure DESC NULLS FIRST, device_code DESC
