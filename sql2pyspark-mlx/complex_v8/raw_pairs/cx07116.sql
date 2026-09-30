WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE site_id <= 3
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM prepared_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        si.zone AS site_zone
    FROM combined_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_zone,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM combined_details
WHERE quality IN ('BAD', 'OK')
ORDER BY quality DESC, id
