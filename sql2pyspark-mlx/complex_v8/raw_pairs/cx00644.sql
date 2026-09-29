SELECT tariff
FROM tariff_windows
WHERE tariff IN ('FLAT', 'TOU')
  AND rate < 5.9
EXCEPT
SELECT tariff
FROM tariff_windows
WHERE rate > 3.7
  AND valid_from < TIMESTAMP '2025-03-09 04:00:00'
ORDER BY tariff
