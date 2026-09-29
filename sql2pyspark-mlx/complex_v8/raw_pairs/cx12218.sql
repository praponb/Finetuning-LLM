SELECT id AS entity_id, tariff
FROM tariff_windows
EXCEPT ALL
SELECT id AS entity_id, tariff
FROM tariff_windows
WHERE MONTH(valid_to) IN (1, 3)
