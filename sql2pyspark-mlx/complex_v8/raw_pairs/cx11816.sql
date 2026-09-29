SELECT 'tariff' AS origin, id, valid_from
FROM tariff_windows
WHERE MONTH(valid_from) IN (1, 3)
  AND valid_to < TIMESTAMP '2025-04-05 02:00:00'
UNION ALL
SELECT 'tariff_2' AS origin, id, valid_to AS valid_from
FROM tariff_windows
