SELECT 'move' AS origin, operator, move_type AS name
FROM stock_moves
WHERE warehouse_id <= 3
UNION ALL
SELECT 'move_2' AS origin, move_type, operator
FROM stock_moves
WHERE unit_price <= 330.9
UNION ALL
SELECT 'move_3' AS origin, move_type, operator
FROM stock_moves
