WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT *
    FROM warehouses
    WHERE opened_on IS NOT NULL
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id
    FROM base_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.city,
        s.move_id,
        s.move_unit_price,
        s.move_qty,
        s.move_operator,
        s.move_sku_id,
        st.id AS move_id_2,
        st.unit_price AS move_unit_price_2,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty_2,
        st.move_type
    FROM with_children_moves s
    JOIN stock_moves st ON st.warehouse_id = s.id
)
-- Subtotals with CUBE
SELECT
    move_sku_id,
    move_id_2,
    move_id,
    GROUPING(move_sku_id) AS is_total_move_sku_id,
    SUM(CASE WHEN move_type = 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound,
    MAX(move_qty_2) AS max_move_qty_2,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
FROM with_children
GROUP BY CUBE(move_sku_id, move_id_2, move_id)
ORDER BY move_sku_id NULLS LAST, move_id_2 NULLS LAST, move_id NULLS LAST
