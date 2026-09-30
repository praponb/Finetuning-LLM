SELECT 'warehouse' AS src, id AS ref_id, wh_code
FROM warehouses
WHERE city IS NOT NULL
UNION ALL
SELECT 'warehouse_2' AS src, capacity, wh_code
FROM warehouses
WHERE capacity >= 4912
  AND LENGTH(TRIM(wh_code)) >= 5
UNION ALL
SELECT 'warehouse_3' AS src, id, city
FROM warehouses
ORDER BY ref_id
