SELECT 'move' AS record_type, qty, moved_at
FROM stock_moves
WHERE unit_price BETWEEN 60.4 AND 298.9
UNION ALL
SELECT 'move_2' AS record_type, qty, moved_at
FROM stock_moves
WHERE warehouse_id IS NOT NULL
  AND operator IN ('op_chan', 'op_eve')
