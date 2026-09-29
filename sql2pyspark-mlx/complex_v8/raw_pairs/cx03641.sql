WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE quality = 'OK'
      AND site_id <= 7
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_label,
        si.tariff AS site_tariff,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM cleaned s
    LEFT JOIN sites si ON si.id = s.site_id AND LENGTH(TRIM(si.site_name)) >= 5
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.site_tariff,
        s.site_name,
        s.site_commissioned_on,
        si.tariff AS site_tariff_2,
        si.site_name AS site_site_name,
        si.installed_kw AS site_installed_kw
    FROM joined_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality, site_installed_kw
    FROM with_details
) src
PIVOT (
    COUNT(site_installed_kw) AS count_v, MAX(site_installed_kw) AS max_v
    FOR quality IN ('OK', 'BAD')
)
ORDER BY read_at
