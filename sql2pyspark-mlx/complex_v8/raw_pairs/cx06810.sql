WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND kwh >= 51.5
)
SELECT id, site_id
FROM scoped_readings
WHERE quality NOT IN ('EST', 'BAD')
UNION ALL
SELECT id, site_id
FROM meter_readings
WHERE voltage > 216.1
