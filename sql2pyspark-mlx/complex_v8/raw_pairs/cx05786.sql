WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND move_type = 'TRANSFER'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), operator ORDER BY qty DESC), 3) AS qty_cume_dist,
        SUM(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_unit_price
    FROM recent
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price_adj,
        s.warehouse_id_sum_unit_price,
        sk.labels AS sku_labels,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM ranked s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.sku_labels,
        s.sku_code,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator
    FROM combined_details s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.sku_code,
        s.move_id,
        s.move_moved_at,
        s.move_sku_id,
        s.move_operator,
        st.id AS move_id_2,
        st.warehouse_id AS move_warehouse_id,
        st.operator AS move_operator_2
    FROM with_children_moves s
    JOIN stock_moves st ON st.sku_id = s.move_sku_id
)
SELECT
    id,
    sku_id,
    qty,
    sku_code,
    move_id,
    move_operator,
    move_id_2,
    move_warehouse_id,
    move_operator_2,
    ABS(qty - 268) AS qty_distance,
    COALESCE(move_operator, 'UNKNOWN') AS move_operator_or_unknown
FROM exploded_detail_moves
ORDER BY move_id DESC
