WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 233.6
)
SELECT id, site_id AS key_id
FROM scoped_readings
WHERE quality IN ('BAD', 'EST')
UNION
SELECT id, site_id
FROM meter_readings
ORDER BY key_id
