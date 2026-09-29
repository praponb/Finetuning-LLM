SELECT qty, operator, id AS entity_id
FROM stock_moves
UNION ALL
SELECT sku_id AS qty, move_type AS operator, id AS entity_id
FROM stock_moves
WHERE operator <> 'op_bee'
  AND MONTH(moved_at) IN (1, 2)
ORDER BY qty DESC
