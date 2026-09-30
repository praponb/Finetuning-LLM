SELECT id, opened_on
FROM warehouses
UNION
SELECT capacity, opened_on
FROM warehouses
ORDER BY id
