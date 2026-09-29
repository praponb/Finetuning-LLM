WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(voltage - 231.7) AS voltage_distance,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE voltage > 219.5
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.kwh_per_voltage,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_details_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 219.5 AND 235.8
        )
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    kwh_per_voltage,
    site_commissioned_on
FROM with_activity
ORDER BY voltage, id
LIMIT 20
