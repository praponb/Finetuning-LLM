SELECT zone AS category
FROM sites
WHERE installed_kw < 284.0
EXCEPT
SELECT site_name AS category
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 5
  AND tariff NOT IN ('FLAT', 'EV')
ORDER BY category
