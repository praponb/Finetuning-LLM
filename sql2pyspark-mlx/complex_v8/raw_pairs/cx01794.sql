WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.kwh_per_voltage,
        si.zone AS site_zone
    FROM prepared s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, kwh_per_voltage, voltage
    FROM with_parent
) src
UNPIVOT (
    value FOR field IN (kwh_per_voltage, voltage)
) unp
ORDER BY id, field
