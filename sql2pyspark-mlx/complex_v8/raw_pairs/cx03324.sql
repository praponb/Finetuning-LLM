SELECT id, rate, 'tariff' AS record_type
FROM tariff_windows
WHERE TO_DATE(valid_from) < DATE '2025-02-18'
UNION
SELECT id, rate, 'tariff_2' AS record_type
FROM tariff_windows
WHERE rate BETWEEN 3.0 AND 5.7
  AND valid_from < TIMESTAMP '2025-02-18 04:00:00'
