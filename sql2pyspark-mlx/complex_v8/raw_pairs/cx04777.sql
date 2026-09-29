SELECT
    operator AS label,
    moved_at,
    move_type AS name,
    'move' AS source
FROM stock_moves
WHERE unit_price >= 86.3
  AND move_type IN ('OUTBOUND', 'INBOUND', 'ADJUST')
UNION ALL
SELECT
    move_type AS label,
    moved_at,
    operator AS name,
    'move_2' AS source
FROM stock_moves
ORDER BY moved_at DESC
