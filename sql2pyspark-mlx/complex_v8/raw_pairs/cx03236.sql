SELECT warehouse_id, moved_at, move_type AS name
FROM stock_moves
UNION ALL
SELECT id, moved_at, operator
FROM stock_moves
ORDER BY warehouse_id DESC
