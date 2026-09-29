SELECT id, moved_at
FROM stock_moves
WHERE qty <= 288
UNION ALL
SELECT qty, moved_at
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-01-19 09:00:00'
UNION ALL
SELECT sku_id, moved_at
FROM stock_moves
ORDER BY id
