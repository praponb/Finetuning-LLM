WITH kept_keys AS (
    -- Set difference on keys
    SELECT device_code, id
    FROM device_readings
    EXCEPT DISTINCT
    SELECT device_code, id
    FROM device_readings
    WHERE pressure BETWEEN 992 AND 1010
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.id = k.id
)
SELECT
    id,
    reading_ts,
    pressure
FROM reconciled
ORDER BY reading_ts DESC NULLS LAST
