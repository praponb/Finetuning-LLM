SELECT operator AS category, move_type AS label, moved_at
FROM stock_moves
WHERE warehouse_id IS NOT NULL
  AND move_type <> 'TRANSFER'
UNION ALL
SELECT move_type, operator, moved_at
FROM stock_moves
WHERE unit_price > 164.1
ORDER BY label DESC, moved_at
