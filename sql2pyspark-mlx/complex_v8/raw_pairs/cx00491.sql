SELECT tariff AS name, id, 'tariff' AS origin
FROM tariff_windows
WHERE valid_to >= TIMESTAMP '2025-01-25 21:00:00'
UNION
SELECT tariff AS name, id, 'tariff_2' AS origin
FROM tariff_windows
ORDER BY origin DESC
