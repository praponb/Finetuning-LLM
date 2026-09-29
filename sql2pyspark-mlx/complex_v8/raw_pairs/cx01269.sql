SELECT id, tariff AS name
FROM tariff_windows
WHERE rate < 5.9
  AND HOUR(valid_from) BETWEEN 8 AND 12
EXCEPT
SELECT id, site_name
FROM sites
