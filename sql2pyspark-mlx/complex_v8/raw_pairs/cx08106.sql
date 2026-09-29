SELECT 'site' AS source, tariff AS category, site_name AS category_2
FROM sites
UNION ALL
SELECT 'site_2' AS source, tariff, site_name
FROM sites
WHERE commissioned_on < DATE '2025-05-10'
  AND UPPER(TRIM(site_name)) LIKE 'D%'
ORDER BY source DESC
