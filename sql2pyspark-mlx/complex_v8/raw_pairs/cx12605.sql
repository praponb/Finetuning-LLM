WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN qty >= 387 THEN 'HIGH' WHEN qty >= 148 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        qty + unit_price AS qty_plus_unit_price,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE qty < 256
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.qty_filled,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.move_type AS move_move_type,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty
    FROM recent_moves s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.move_type NOT IN ('INBOUND', 'OUTBOUND')
)
-- Subtotals with CUBE
SELECT
    move_warehouse_id,
    move_type,
    MIN(unit_price) AS min_unit_price,
    COUNT(*) AS cnt
FROM with_children
GROUP BY CUBE(move_warehouse_id, move_type)
ORDER BY move_warehouse_id NULLS LAST, move_type NULLS LAST
