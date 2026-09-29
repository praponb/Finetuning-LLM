SELECT operator, moved_at AS happened_at, 'move' AS src
FROM stock_moves
UNION ALL
SELECT move_type AS operator, moved_at AS happened_at, 'move_2' AS src
FROM stock_moves
UNION ALL
SELECT move_type AS operator, moved_at AS happened_at, 'move_3' AS src
FROM stock_moves
ORDER BY src DESC
