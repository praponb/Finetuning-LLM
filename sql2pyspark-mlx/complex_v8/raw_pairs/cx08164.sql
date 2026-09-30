SELECT
    sku_id,
    operator AS category,
    moved_at AS happened_at,
    'move' AS record_type
FROM stock_moves
UNION ALL
SELECT
    warehouse_id,
    move_type,
    moved_at,
    'move_2' AS record_type
FROM stock_moves
ORDER BY record_type DESC
