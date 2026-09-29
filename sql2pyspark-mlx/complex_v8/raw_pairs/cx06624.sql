SELECT id
FROM tariff_windows
WHERE valid_from >= TIMESTAMP '2025-02-15 04:00:00'
INTERSECT
SELECT id
FROM tariff_windows
WHERE rate BETWEEN 2.0 AND 7.6
  AND MONTH(valid_from) IN (1, 2)
