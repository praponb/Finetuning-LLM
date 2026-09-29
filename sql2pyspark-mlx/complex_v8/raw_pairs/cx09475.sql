SELECT qty, moved_at AS happened_at
FROM stock_moves
UNION ALL
SELECT warehouse_id AS qty, moved_at AS happened_at
FROM stock_moves
