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
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE (operator = 'op_eve' OR TO_DATE(moved_at) < DATE '2025-03-02')
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_filled,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM recent
    WHERE unit_price >= 160.8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        qty_filled,
        move_type_slug,
        ROUND(AVG(qty_filled) OVER w_warehouse_id_sku_id, 2) AS warehouse_id_sku_id_avg_qty_filled,
        SIZE(COLLECT_SET(move_type) OVER w_warehouse_id_sku_id) AS distinct_move_type_in_warehouse_id_sku_id,
        SUM(qty_filled) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_filled,
        SUM(qty) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_sum_qty
    FROM enriched
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.qty_filled,
        s.move_type_slug,
        s.warehouse_id_sku_id_avg_qty_filled,
        s.distinct_move_type_in_warehouse_id_sku_id,
        s.running_qty_filled,
        s.warehouse_id_sku_id_sum_qty,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.qty AS move_qty,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM sequenced s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.qty >= 98
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    COALESCE(move_move_type, 'ALL') AS move_move_type,
    GROUPING_ID() AS grouping_level,
    MAX(moved_at) AS last_moved_at
FROM exploded_detail_moves
GROUP BY ROLLUP(move_type, move_move_type)
