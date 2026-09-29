WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE voltage > 214.3
),
enriched_site AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, site_id, quality, site_installed_kw
    FROM enriched_site
) src
PIVOT (
    AVG(site_installed_kw)
    FOR quality IN ('OK', 'EST', 'BAD')
)
ORDER BY read_at, site_id
