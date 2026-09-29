SELECT 'move' AS src, warehouse_id, moved_at AS happened_at
FROM stock_moves
UNION
SELECT 'move_2' AS src, id AS warehouse_id, moved_at AS happened_at
FROM stock_moves
WHERE move_type NOT IN ('INBOUND', 'TRANSFER')
