SELECT id AS ref_id, wh_code AS category
FROM warehouses
INTERSECT
SELECT sku_id, operator
FROM stock_moves
ORDER BY ref_id
