SELECT id
FROM tariff_windows
EXCEPT
SELECT id
FROM tariff_windows
WHERE rate >= 3.7
  AND tariff = 'TOU'
