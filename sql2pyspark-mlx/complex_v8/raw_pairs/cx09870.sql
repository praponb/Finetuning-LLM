SELECT warehouse_id, unit_price, 'move' AS origin
FROM stock_moves
UNION ALL
SELECT id AS warehouse_id, unit_cost AS unit_price, 'sku' AS origin
FROM skus
ORDER BY unit_price
