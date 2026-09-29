SELECT id AS ref_id, sku_id AS qty, operator AS name
FROM stock_moves
WHERE warehouse_id <= 3
UNION ALL
SELECT capacity, id, city
FROM warehouses
ORDER BY ref_id
