SELECT id, rate
FROM tariff_windows
WHERE valid_to < TIMESTAMP '2025-03-09 04:00:00'
  AND valid_from < TIMESTAMP '2025-03-09 05:00:00'
UNION ALL
SELECT id, rate
FROM tariff_windows
WHERE HOUR(valid_from) BETWEEN 7 AND 13
