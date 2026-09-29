SELECT id, tariff AS category, 'site' AS record_type
FROM sites
WHERE commissioned_on < DATE '2024-11-21'
UNION ALL
SELECT id, zone, 'site_2' AS record_type
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'S%'
  AND commissioned_on IS NOT NULL
