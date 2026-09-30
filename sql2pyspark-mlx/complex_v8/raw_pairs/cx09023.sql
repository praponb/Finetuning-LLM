WITH filtered_moves AS (
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
        QUARTER(moved_at) AS moved_at_quarter,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM stock_moves
    WHERE unit_price < 293.2
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_quarter,
        s.unit_price_adj,
        w.city AS warehouse_city,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels
    FROM filtered_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.wh_code IS NOT NULL
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price > 136.0
              AND st.operator NOT IN ('op_chan', 'op_dew')
        )
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.warehouse_city,
        s.sku_is_active,
        s.sku_labels,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type
    FROM with_activity s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    sku_is_active,
    sku_labels,
    move_id,
    move_operator,
    move_move_type,
    CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag
FROM with_children_moves
ORDER BY warehouse_id
