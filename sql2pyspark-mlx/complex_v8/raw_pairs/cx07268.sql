SELECT
    operator AS name,
    move_type AS label,
    moved_at AS event_time,
    'move' AS src
FROM stock_moves
UNION ALL
SELECT
    operator,
    move_type,
    moved_at,
    'move_2' AS src
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-02-06 07:00:00'
  AND qty >= 138
UNION ALL
SELECT
    operator,
    move_type,
    moved_at,
    'move_3' AS src
FROM stock_moves
