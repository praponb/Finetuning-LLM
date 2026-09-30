SELECT qty, moved_at
FROM stock_moves
WHERE sku_id <= 7
UNION ALL
SELECT id AS qty, moved_at
FROM stock_moves
WHERE moved_at < TIMESTAMP '2025-03-02 12:00:00'
UNION ALL
SELECT id AS qty, moved_at
FROM stock_moves
ORDER BY moved_at
