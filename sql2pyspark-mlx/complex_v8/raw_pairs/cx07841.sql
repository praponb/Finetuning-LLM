WITH prepared_sites AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE zone NOT IN ('Z2', 'Z4')
)
SELECT 'site' AS origin, tariff AS name, site_name AS category
FROM prepared_sites
WHERE zone IN ('Z2', 'Z4')
UNION ALL
SELECT 'site_2' AS origin, zone AS name, site_name AS category
FROM sites
UNION ALL
SELECT 'site_3' AS origin, zone AS name, tariff AS category
FROM sites
WHERE commissioned_on < DATE '2024-11-21'
  AND zone IN ('Z4', 'Z2')
