WITH base_moves AS (
    -- Filter stock_moves
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
    WHERE warehouse_id <= 5
      AND sku_id >= 10
),
joined AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM base_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.sku_unit_cost,
        s.sku_category,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id
    FROM joined s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.sku_unit_cost,
        s.move_id,
        s.move_unit_price,
        s.move_qty,
        s.move_operator,
        s.move_warehouse_id,
        st.id AS move_id_2,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty_2,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price_2
    FROM line_level s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id IS NOT NULL
),
with_details AS (
    -- Attach sku attributes
    SELECT s.*, sk.labels AS sku_labels
    FROM detail_rows s
    LEFT JOIN skus sk ON sk.id = s.move_sku_id
),
per_label AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.sku_unit_cost,
        s.move_id,
        s.move_unit_price,
        s.move_qty,
        s.move_operator,
        s.move_warehouse_id,
        s.move_id_2,
        s.move_moved_at,
        s.move_qty_2,
        s.move_sku_id,
        s.move_unit_price_2,
        t.sku_label
    FROM with_details s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(qty) OVER (PARTITION BY move_operator ORDER BY move_qty_2 DESC, id, move_id, move_id_2, sku_label ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, move_sku_id ORDER BY sku_unit_cost), 3) AS sku_unit_cost_percent_rank,
        COUNT(*) OVER (PARTITION BY sku_label, move_operator) AS rows_in_sku_label_move_operator,
        SIZE(COLLECT_SET(move_warehouse_id) OVER (PARTITION BY sku_label, move_operator)) AS distinct_move_warehouse_id_in_sku_label_move_ope
    FROM per_label
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    move_id,
    move_qty,
    move_operator,
    move_warehouse_id,
    move_id_2,
    move_moved_at,
    move_unit_price_2,
    sku_label,
    qty_avg_4,
    distinct_move_warehouse_id_in_sku_label_move_ope,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    FLOOR(qty / 100) * 100 AS qty_bucket
FROM windowed
WHERE warehouse_id >= 3
