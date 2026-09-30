WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 238.8
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.site_name
    FROM base s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw >= 93.3
    WHERE si.site_name IS NOT NULL
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_zone,
    site_tariff,
    site_name,
    ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
FROM with_details_site
ORDER BY voltage, voltage_per_kwh
