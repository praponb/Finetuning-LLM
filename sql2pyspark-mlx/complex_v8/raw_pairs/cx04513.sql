WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
)
SELECT
    id,
    kwh,
    voltage,
    quality
FROM scoped
WHERE quality IS NOT NULL
ORDER BY kwh, voltage DESC
