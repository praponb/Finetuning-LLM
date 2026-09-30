SELECT
    'move' AS origin,
    move_type AS name,
    moved_at,
    operator AS category
FROM stock_moves
WHERE sku_id IS NOT NULL
UNION ALL
SELECT
    'move_2' AS origin,
    move_type AS name,
    moved_at,
    operator AS category
FROM stock_moves
WHERE unit_price > 189.4
ORDER BY category DESC
