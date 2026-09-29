SELECT id, zone AS name, commissioned_on
FROM sites
WHERE installed_kw > 141.6
  AND zone IS NOT NULL
UNION ALL
SELECT id, site_name AS name, commissioned_on
FROM sites
WHERE installed_kw > 141.6
  AND site_name IS NOT NULL
UNION ALL
SELECT id, tariff AS name, commissioned_on
FROM sites
WHERE site_name IS NOT NULL
  AND tariff IN ('FLAT', 'EV')
ORDER BY commissioned_on, id
