SELECT site_name, commissioned_on AS day, id AS qty
FROM sites
WHERE tariff = 'TOU'
  AND installed_kw >= 93.3
UNION ALL
SELECT site_name, commissioned_on, id
FROM sites
UNION ALL
SELECT tariff, commissioned_on, id
FROM sites
WHERE installed_kw > 96.9
ORDER BY site_name DESC, qty
