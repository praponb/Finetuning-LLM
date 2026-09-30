SELECT move_type, moved_at AS event_time
FROM stock_moves
WHERE operator = 'op_dew'
  AND TO_DATE(moved_at) < DATE '2025-03-08'
UNION
SELECT operator, moved_at
FROM stock_moves
ORDER BY move_type DESC, event_time
