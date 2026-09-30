WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 109.0
)
SELECT site_id AS key_id, id, quality
FROM scoped_readings
UNION ALL
SELECT site_id, id, quality
FROM meter_readings
UNION ALL
SELECT id, site_id, quality
FROM meter_readings
ORDER BY quality, id
