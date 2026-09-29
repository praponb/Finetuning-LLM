SELECT id, sku_code AS name
FROM skus
UNION ALL
SELECT sku_id, operator
FROM stock_moves
WHERE qty BETWEEN 78 AND 415
UNION ALL
SELECT warehouse_id, operator
FROM stock_moves
