SELECT id AS key_id, operator AS category
FROM stock_moves
WHERE move_type = 'TRANSFER'
INTERSECT
SELECT capacity, wh_code
FROM warehouses
ORDER BY key_id, category
