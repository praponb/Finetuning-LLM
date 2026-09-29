SELECT operator AS name, moved_at
FROM stock_moves
UNION
SELECT move_type AS name, moved_at
FROM stock_moves
WHERE sku_id IS NOT NULL
  AND operator = 'op_ann'
