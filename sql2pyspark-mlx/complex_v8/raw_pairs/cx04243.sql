SELECT sku_id, moved_at AS happened_at, 'move' AS origin
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-01-31 07:00:00'
UNION ALL
SELECT warehouse_id, moved_at, 'move_2' AS origin
FROM stock_moves
UNION ALL
SELECT id, moved_at, 'move_3' AS origin
FROM stock_moves
