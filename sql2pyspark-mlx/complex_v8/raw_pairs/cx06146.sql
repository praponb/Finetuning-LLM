SELECT tariff
FROM tariff_windows
INTERSECT ALL
SELECT zone
FROM sites
ORDER BY tariff
