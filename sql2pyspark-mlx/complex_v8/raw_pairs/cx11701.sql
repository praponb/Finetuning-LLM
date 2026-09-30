SELECT tariff, zone, installed_kw AS metric
FROM sites
WHERE YEAR(commissioned_on) = 2025
UNION ALL
SELECT zone, tariff, installed_kw
FROM sites
WHERE LOWER(site_name) LIKE '%ea%'
  AND installed_kw > 96.9
UNION ALL
SELECT site_name, zone, installed_kw
FROM sites
WHERE tariff NOT IN ('FLAT', 'EV')
  AND commissioned_on < DATE '2025-05-07'
ORDER BY metric, tariff
