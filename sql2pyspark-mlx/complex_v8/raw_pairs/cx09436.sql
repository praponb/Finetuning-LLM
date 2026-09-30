SELECT 'warehouse' AS src, id, capacity
FROM warehouses
UNION
SELECT 'warehouse_2' AS src, id, capacity
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
ORDER BY id
