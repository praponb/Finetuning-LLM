WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref,
        ABS(unit_price - 248.3) AS unit_price_distance
    FROM stock_moves
    WHERE (move_type IN ('ADJUST', 'TRANSFER') OR qty >= 112)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        unit_price_distance,
        sku_code,
        warehouse_capacity,
        warehouse_city,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY sku_category, sku_id), 0), 4) AS warehouse_capacity_share_of_sku_category_sku_id,
        MIN(unit_price) OVER (ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        NTILE(5) OVER (ORDER BY unit_price DESC NULLS FIRST, id) AS unit_price_quintile,
        SUM(sku_weight_kg) OVER (PARTITION BY move_type ORDER BY unit_price_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sku_weight_kg_sum_7,
        ROUND(AVG(unit_price_distance) OVER (PARTITION BY operator, sku_category), 2) AS operator_sku_category_avg_unit_price_distance,
        MAX(qty) OVER w_operator_sku_category AS operator_sku_category_max_qty,
        SIZE(COLLECT_SET(warehouse_city) OVER (PARTITION BY operator, sku_category)) AS distinct_warehouse_city_in_operator_sku_category,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY unit_price_distance DESC), 3) AS unit_price_distance_cume_dist,
        SIZE(COLLECT_SET(warehouse_city) OVER w_sku_category_sku_id) AS distinct_warehouse_city_in_sku_category_sku_id,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, sku_category), 2) AS qty_vs_operator_sku_category_avg,
        ROW_NUMBER() OVER w_all_by_unit_price_2 AS rn,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7
    FROM enriched
    WINDOW w_operator_sku_category AS (PARTITION BY operator, sku_category), w_sku_category_sku_id AS (PARTITION BY sku_category, sku_id), w_all_by_unit_price_2 AS (ORDER BY unit_price ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        unit_price_distance,
        sku_code,
        warehouse_capacity,
        warehouse_city,
        warehouse_capacity_share_of_sku_category_sku_id,
        min_unit_price_so_far,
        unit_price_quintile,
        sku_weight_kg_sum_7,
        operator_sku_category_avg_unit_price_distance,
        operator_sku_category_max_qty,
        distinct_warehouse_city_in_operator_sku_category,
        unit_price_distance_cume_dist,
        distinct_warehouse_city_in_sku_category_sku_id,
        qty_vs_operator_sku_category_avg,
        unit_price_avg_7
    FROM with_trends
    WHERE rn <= 5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price_avg_7 / NULLIF(SUM(unit_price_avg_7) OVER w_warehouse_city_unit_price_qu, 0), 4) AS unit_price_avg_7_share_of_warehouse_city_unit_pr,
        LEAD(unit_price_quintile) OVER (PARTITION BY sku_id ORDER BY operator_sku_category_max_qty NULLS LAST, id) AS next_unit_price_quintile,
        LAST_VALUE(unit_price) OVER (PARTITION BY sku_id, distinct_warehouse_city_in_operator_sku_category ORDER BY warehouse_capacity_share_of_sku_category_sku_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM top_per_group
    WINDOW w_warehouse_city_unit_price_qu AS (PARTITION BY warehouse_city, unit_price_quintile)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    unit_price_distance,
    warehouse_capacity,
    warehouse_city,
    min_unit_price_so_far,
    unit_price_quintile,
    operator_sku_category_avg_unit_price_distance,
    operator_sku_category_max_qty,
    qty_vs_operator_sku_category_avg,
    unit_price_avg_7_share_of_warehouse_city_unit_pr,
    next_unit_price_quintile,
    last_unit_price,
    GREATEST(warehouse_capacity, unit_price_distance) AS greatest_warehouse_capacity_unit_price_distance
FROM ranked
