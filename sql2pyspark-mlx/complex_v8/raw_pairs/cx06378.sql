WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        ABS(qty - 348) AS qty_distance
    FROM stock_moves
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.move_type_clean,
        s.qty_distance,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_sku_category_warehouse_id_by AS unit_price_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY sku_category, warehouse_id ORDER BY unit_price DESC, id) AS unit_price_seq,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER (PARTITION BY warehouse_id), 0), 4) AS sku_weight_kg_share_of_warehouse_id,
        NTH_VALUE(qty_distance, 2) OVER (PARTITION BY sku_category, warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_distance,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY sku_category ORDER BY qty_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAG(unit_price, 3) OVER (PARTITION BY sku_category, warehouse_id ORDER BY unit_price DESC, id) AS prev3_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id
    FROM with_details
    WINDOW w_sku_category_warehouse_id_by AS (PARTITION BY sku_category, warehouse_id ORDER BY unit_price DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price,
        move_type_clean,
        qty_distance,
        sku_weight_kg,
        sku_category,
        unit_price_seq,
        sku_weight_kg_share_of_warehouse_id,
        second_qty_distance,
        rows_in_warehouse_id,
        first_known_unit_price,
        prev3_unit_price,
        distinct_sku_id_in_warehouse_id
    FROM windowed
    WHERE unit_price_dense_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        move_type_clean,
        sku_weight_kg,
        sku_category,
        unit_price_seq,
        sku_weight_kg_share_of_warehouse_id,
        second_qty_distance,
        prev3_unit_price,
        distinct_sku_id_in_warehouse_id,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_category), 2) AS sku_category_avg_unit_price,
        COUNT(*) OVER w_sku_category AS rows_in_sku_category,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_category, operator ORDER BY sku_weight_kg ASC, id), 3) AS sku_weight_kg_percent_rank,
        DENSE_RANK() OVER (ORDER BY unit_price, id) AS unit_price_dense_rank,
        LAST_VALUE(second_qty_distance) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_qty_distance
    FROM top_rows
    WINDOW w_sku_category AS (PARTITION BY sku_category)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type_clean,
    sku_weight_kg,
    sku_category,
    unit_price_seq,
    second_qty_distance,
    prev3_unit_price,
    distinct_sku_id_in_warehouse_id,
    sku_category_avg_unit_price,
    rows_in_sku_category,
    sku_weight_kg_percent_rank,
    unit_price_dense_rank,
    ABS(unit_price_seq - 4) AS unit_price_seq_distance
FROM with_windows
WHERE sku_category_avg_unit_price > 50.9
ORDER BY id, unit_price_seq NULLS LAST
