SELECT zone, tariff
FROM sites
WHERE tariff IN ('FLAT', 'EV')
  AND UPPER(TRIM(site_name)) LIKE 'R%'
UNION ALL
SELECT tariff AS zone, zone AS tariff
FROM sites
ORDER BY tariff DESC
