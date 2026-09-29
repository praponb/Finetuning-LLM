WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'M%'
)
SELECT id AS entity_id
FROM scoped_sites
INTERSECT
SELECT id
FROM tariff_windows
ORDER BY entity_id
