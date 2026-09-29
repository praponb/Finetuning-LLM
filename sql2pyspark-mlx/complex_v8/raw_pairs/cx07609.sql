SELECT id, tariff AS category
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'D%'
INTERSECT
SELECT id, site_name AS category
FROM sites
ORDER BY category, id
