WITH scoped_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM sites
    WHERE tariff NOT IN ('FLAT', 'TOU')
)
SELECT site_name AS name, zone, commissioned_on
FROM scoped_sites
UNION
SELECT site_name, tariff, commissioned_on
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 7
  AND tariff = 'TOU'
ORDER BY name DESC
