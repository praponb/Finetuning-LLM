WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND warehouse_id <= 5
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        w.city AS warehouse_city
    FROM cleaned s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price,
        sku_category,
        sku_weight_kg,
        warehouse_city,
        SUM(sku_weight_kg) OVER w_sku_id AS sku_id_sum_sku_weight_kg,
        NTILE(4) OVER (PARTITION BY sku_id, operator ORDER BY sku_weight_kg, id) AS sku_weight_kg_quartile,
        LAG(moved_at) OVER w_all_by_unit_price AS prev_moved_at,
        COUNT(*) OVER (PARTITION BY sku_category) AS rows_in_sku_category,
        SIZE(COLLECT_SET(operator) OVER w_sku_id) AS distinct_operator_in_sku_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_city ORDER BY sku_weight_kg NULLS LAST, id), 3) AS sku_weight_kg_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_unit_price_ties, 3) AS unit_price_cume_dist,
        SIZE(COLLECT_SET(warehouse_city) OVER w_sku_category) AS distinct_warehouse_city_in_sku_category,
        RANK() OVER (PARTITION BY operator, sku_category ORDER BY moved_at, id) AS moved_at_rank
    FROM enriched_sku
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_all_by_unit_price AS (ORDER BY unit_price DESC, id), w_all_by_unit_price_ties AS (ORDER BY unit_price DESC), w_sku_category AS (PARTITION BY sku_category)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE moved_at_rank <= 3
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    unit_price,
    sku_category,
    sku_weight_kg,
    sku_id_sum_sku_weight_kg,
    prev_moved_at,
    rows_in_sku_category,
    sku_weight_kg_percent_rank,
    unit_price_cume_dist,
    distinct_warehouse_city_in_sku_category,
    moved_at_rank,
    FLOOR(moved_at_rank / 2) * 2 AS moved_at_rank_bucket
FROM first_rows
ORDER BY sku_category
