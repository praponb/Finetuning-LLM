SELECT sku_id AS key_id, unit_price, moved_at
FROM stock_moves
WHERE move_type IS NOT NULL
UNION ALL
SELECT id AS key_id, unit_price, moved_at
FROM stock_moves
ORDER BY key_id, unit_price
