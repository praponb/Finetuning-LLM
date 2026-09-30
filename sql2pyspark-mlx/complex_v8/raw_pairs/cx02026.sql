SELECT id AS qty, capacity, 'warehouse' AS origin
FROM warehouses
UNION ALL
SELECT capacity, id, 'warehouse_2' AS origin
FROM warehouses
