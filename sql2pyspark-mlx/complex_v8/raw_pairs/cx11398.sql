SELECT id AS ref_id
FROM tariff_windows
INTERSECT
SELECT id AS ref_id
FROM tariff_windows
WHERE rate BETWEEN 2.2 AND 6.1
ORDER BY ref_id
