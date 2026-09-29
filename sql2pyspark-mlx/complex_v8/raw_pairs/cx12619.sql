WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM scoped s
    JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'S%'
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.site_commissioned_on,
        s.site_zone,
        si.installed_kw AS site_installed_kw
    FROM enriched_site s
    JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT site_commissioned_on, site_id, site_zone, kwh
    FROM joined
) src
PIVOT (
    SUM(kwh) AS amount, COUNT(kwh) AS n
    FOR site_zone IN ('Z2', 'Z4', 'Z3')
)
ORDER BY site_commissioned_on, site_id
