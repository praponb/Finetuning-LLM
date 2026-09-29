SELECT 'site' AS origin, id AS qty, installed_kw AS amount
FROM sites
UNION ALL
SELECT 'tariff' AS origin, id, rate
FROM tariff_windows
WHERE tariff = 'TOU'
  AND valid_to >= TIMESTAMP '2025-02-16 05:00:00'
ORDER BY qty, amount
