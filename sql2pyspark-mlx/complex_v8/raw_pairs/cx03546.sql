SELECT id AS qty
FROM tariff_windows
WHERE MONTH(valid_from) IN (1, 3)
EXCEPT
SELECT id
FROM tariff_windows
