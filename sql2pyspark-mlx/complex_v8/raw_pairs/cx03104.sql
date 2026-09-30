WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE (sku_id <= 16 OR TO_DATE(moved_at) < DATE '2025-03-14')
)
SELECT qty AS ref_id, moved_at AS event_time, 'move' AS origin
FROM scoped
UNION ALL
SELECT qty, moved_at, 'move_2' AS origin
FROM stock_moves
WHERE sku_id IS NOT NULL
