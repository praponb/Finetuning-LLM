WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR warehouse_id >= 2)
)
SELECT
    'move' AS source,
    sku_id,
    move_type AS label,
    moved_at AS event_time
FROM cleaned
UNION ALL
SELECT
    'move_2' AS source,
    qty,
    move_type,
    moved_at
FROM stock_moves
UNION ALL
SELECT
    'move_3' AS source,
    warehouse_id,
    move_type,
    moved_at
FROM stock_moves
ORDER BY sku_id DESC, label
