SELECT
    qty AS key_id,
    moved_at AS happened_at,
    operator,
    'move' AS source
FROM stock_moves
UNION ALL
SELECT
    id,
    moved_at,
    move_type,
    'move_2' AS source
FROM stock_moves
WHERE moved_at < TIMESTAMP '2025-03-08 11:00:00'
  AND move_type IS NOT NULL
