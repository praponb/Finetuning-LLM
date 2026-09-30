SELECT
    tariff AS name,
    zone AS label,
    commissioned_on AS day,
    'site' AS source
FROM sites
WHERE zone = 'Z2'
UNION ALL
SELECT
    site_name AS name,
    zone AS label,
    commissioned_on AS day,
    'site_2' AS source
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'D%'
  AND zone = 'Z2'
ORDER BY source
