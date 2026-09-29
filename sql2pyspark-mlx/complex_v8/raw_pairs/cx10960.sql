SELECT id AS qty, zone AS category, 'site' AS origin
FROM sites
WHERE installed_kw > 96.9
UNION ALL
SELECT id AS qty, tariff AS category, 'tariff' AS origin
FROM tariff_windows
UNION ALL
SELECT id AS qty, tariff AS category, 'tariff' AS origin
FROM tariff_windows
WHERE valid_from >= TIMESTAMP '2025-02-15 04:00:00'
