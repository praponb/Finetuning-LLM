SELECT tariff
FROM tariff_windows
EXCEPT
SELECT tariff
FROM tariff_windows
WHERE tariff <> 'TOU'
  AND TO_DATE(valid_to) < DATE '2025-04-05'
