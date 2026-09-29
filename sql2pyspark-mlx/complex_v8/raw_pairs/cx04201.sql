WITH prepared_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        LENGTH(TRIM(site_name)) AS site_name_len
    FROM sites
)
SELECT zone AS name, site_name AS category
FROM prepared_sites
UNION ALL
SELECT site_name AS name, tariff AS category
FROM sites
UNION ALL
SELECT tariff AS name, zone AS category
FROM sites
ORDER BY category, name
