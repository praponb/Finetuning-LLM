WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 13:00:00'
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type = 'ADJUST'
        )
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.unit_price_adj,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price
    FROM qualified s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price_adj,
    move_id,
    move_moved_at,
    move_sku_id,
    FLOOR(qty / 100) * 100 AS qty_bucket,
    move_qty - unit_price AS move_qty_minus_unit_price
FROM line_level
WHERE warehouse_id >= 3
ORDER BY move_sku_id DESC, qty_bucket NULLS LAST, id, move_id
LIMIT 10
