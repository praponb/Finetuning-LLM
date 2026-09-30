SELECT id AS qty
FROM sites
INTERSECT
SELECT id AS qty
FROM tariff_windows
WHERE tariff = 'EV'
  AND HOUR(valid_from) BETWEEN 10 AND 15
ORDER BY qty
