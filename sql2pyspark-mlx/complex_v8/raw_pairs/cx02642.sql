WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.days_since_moved_at,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM prepared_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.unit_price < 341.5
)
SELECT
    move_operator,
    move_id,
    move_type,
    MIN(days_since_moved_at) AS min_days_since_moved_at,
    ROUND(STDDEV(move_qty), 2) AS stddev_move_qty
FROM with_children_moves
GROUP BY ROLLUP(move_operator, move_id, move_type)
ORDER BY move_operator NULLS LAST, move_id NULLS LAST, move_type NULLS LAST
