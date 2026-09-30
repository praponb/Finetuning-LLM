SELECT 'warehouse' AS src, id, capacity
FROM warehouses
UNION ALL
SELECT 'warehouse_2' AS src, id, capacity
FROM warehouses
WHERE YEAR(opened_on) = 2025
ORDER BY capacity
