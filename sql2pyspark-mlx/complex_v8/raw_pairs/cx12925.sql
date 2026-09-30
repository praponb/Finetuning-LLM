SELECT tariff
FROM tariff_windows
WHERE HOUR(valid_from) BETWEEN 10 AND 14
EXCEPT
SELECT zone AS tariff
FROM sites
ORDER BY tariff
