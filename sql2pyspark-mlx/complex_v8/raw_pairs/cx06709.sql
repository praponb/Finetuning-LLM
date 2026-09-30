WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 241.8
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.zone AS site_zone,
        si.site_name
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.site_zone,
        s.site_name,
        si.zone AS site_zone_2
    FROM with_parent_site s
    JOIN sites si ON si.id = s.site_id
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.site_zone,
        s.site_name,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone_3
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, site_tariff, site_installed_kw
    FROM joined_site
) src
PIVOT (
    COUNT(site_installed_kw)
    FOR site_tariff IN ('EV', 'TOU', 'FLAT')
)
