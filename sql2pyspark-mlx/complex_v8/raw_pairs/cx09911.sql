WITH base AS (
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
    WHERE warehouse_id IS NOT NULL
),
joined AS (
    -- Join warehouses
    SELECT
        s.*,
        w.city AS warehouse_city,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        warehouse_city,
        sku_weight_kg,
        SIZE(COLLECT_SET(sku_category) OVER w_move_type) AS distinct_sku_category_in_move_type,
        ROUND(AVG(unit_price) OVER (ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        NTILE(4) OVER (ORDER BY qty DESC, id) AS qty_quartile,
        SUM(sku_weight_kg) OVER (PARTITION BY operator ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_weight_kg,
        NTILE(5) OVER (PARTITION BY operator ORDER BY sku_weight_kg, id) AS sku_weight_kg_quintile,
        SUM(unit_price) OVER (PARTITION BY move_type) AS move_type_sum_unit_price,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id ORDER BY sku_weight_kg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_avg_1_prior,
        NTILE(5) OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at ASC, id) AS moved_at_quintile,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY sku_weight_kg) AS sku_weight_kg_dense_rank,
        ROUND(AVG(qty) OVER (PARTITION BY operator ORDER BY sku_weight_kg, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior,
        MIN(sku_weight_kg) OVER w_warehouse_city_warehouse_id AS warehouse_city_warehouse_id_min_sku_weight_kg,
        ROW_NUMBER() OVER w_sku_id_by_unit_price AS unit_price_seq,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY warehouse_id ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg_2,
        ROUND(CUME_DIST() OVER w_operator_warehouse_id_by_mov, 3) AS moved_at_cume_dist
    FROM joined
    WINDOW w_move_type AS (PARTITION BY move_type), w_warehouse_city_warehouse_id AS (PARTITION BY warehouse_city, warehouse_id), w_sku_id_by_unit_price AS (PARTITION BY sku_id ORDER BY unit_price, id), w_operator_warehouse_id_by_mov AS (PARTITION BY operator, warehouse_id ORDER BY moved_at ASC, id)
)
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    sku_weight_kg,
    distinct_sku_category_in_move_type,
    unit_price_avg_4,
    qty_quartile,
    running_sku_weight_kg,
    sku_weight_kg_quintile,
    move_type_sum_unit_price,
    sku_weight_kg_avg_1_prior,
    moved_at_quintile,
    second_sku_weight_kg,
    sku_weight_kg_dense_rank,
    qty_avg_3_prior,
    warehouse_city_warehouse_id_min_sku_weight_kg,
    unit_price_seq,
    moved_at_cume_dist,
    LOWER(REGEXP_REPLACE(TRIM(warehouse_city), ' ', '_')) AS warehouse_city_slug,
    SUBSTRING(TRIM(warehouse_city), 1, 2) AS warehouse_city_prefix
FROM windowed
WHERE sku_id >= 11
