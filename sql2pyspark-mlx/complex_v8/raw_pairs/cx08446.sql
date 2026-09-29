WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.kwh_per_voltage,
        si.site_name,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM cleaned s
    LEFT JOIN sites si ON si.id = s.site_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        kwh_per_voltage,
        site_name,
        site_tariff,
        site_zone,
        COALESCE(kwh_per_voltage, 0.0) AS kwh_per_voltage_filled
    FROM enriched
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, site_tariff, site_zone, kwh_per_voltage_filled
    FROM derived
) src
PIVOT (
    SUM(kwh_per_voltage_filled)
    FOR site_zone IN ('Z2', 'Z3')
)
