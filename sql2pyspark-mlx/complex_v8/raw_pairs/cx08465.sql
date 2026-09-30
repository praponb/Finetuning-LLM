WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM stock_moves
    WHERE (unit_price <= 313.8 OR warehouse_id <= 3)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_qty
    FROM base s
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.greatest_unit_price_qty,
        s.move_count,
        s.total_move_qty,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM with_stats s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category IN ('Toys', 'Beauty')
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.move_count,
        s.sku_code,
        s.sku_unit_cost,
        s.sku_category,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type
    FROM enriched s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', sku_category, sku_code) AS sku_category_sku_code_label,
        move_count - sku_unit_cost AS move_count_minus_sku_unit_cost
    FROM exploded_detail_moves
)
SELECT
    sku_category,
    warehouse_id,
    COUNT(*) FILTER (WHERE move_type <> 'INBOUND') AS n_move_type_inbound,
    COUNT(DISTINCT id) AS distinct_ids
FROM annotated
GROUP BY GROUPING SETS ((sku_category, warehouse_id), (sku_category), (warehouse_id), ())
ORDER BY sku_category NULLS LAST, warehouse_id NULLS LAST
