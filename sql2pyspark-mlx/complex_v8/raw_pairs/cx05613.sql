WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage <= 241.9
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_slug,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
joined_site_2 AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.site_installed_kw,
        si.zone AS site_zone_2,
        si.tariff AS site_tariff
    FROM joined_site s
    JOIN sites si ON si.id = s.site_id
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.site_installed_kw,
        s.site_tariff,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM joined_site_2 s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT *
FROM (
    SELECT reading_read_at, site_tariff, site_installed_kw
    FROM with_children
) src
PIVOT (
    MAX(site_installed_kw)
    FOR site_tariff IN ('FLAT', 'TOU', 'EV')
)
