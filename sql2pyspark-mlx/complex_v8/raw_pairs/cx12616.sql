SELECT move_type, sku_id AS key_id
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-01-31 10:00:00'
UNION ALL
SELECT operator AS move_type, id AS key_id
FROM stock_moves
ORDER BY move_type, key_id
