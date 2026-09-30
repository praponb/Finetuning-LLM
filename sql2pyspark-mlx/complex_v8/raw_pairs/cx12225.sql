SELECT id, capacity
FROM warehouses
UNION
SELECT capacity, id
FROM warehouses
