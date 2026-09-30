SELECT 'warehouse' AS src, id, opened_on AS day
FROM warehouses
WHERE city IS NOT NULL
  AND UPPER(TRIM(wh_code)) LIKE 'K%'
UNION ALL
SELECT 'warehouse_2' AS src, id, opened_on
FROM warehouses
UNION ALL
SELECT 'warehouse_3' AS src, capacity, opened_on
FROM warehouses
ORDER BY src
