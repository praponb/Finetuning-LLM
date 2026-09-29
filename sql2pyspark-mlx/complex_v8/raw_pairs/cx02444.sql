SELECT qty, moved_at AS happened_at
FROM stock_moves
WHERE TO_DATE(moved_at) < DATE '2025-02-06'
UNION ALL
SELECT qty, moved_at
FROM stock_moves
