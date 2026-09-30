WITH base AS (
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
        CASE WHEN unit_price >= 317.0 THEN 'A' WHEN unit_price >= 189.4 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE (HOUR(moved_at) BETWEEN 7 AND 9 OR sku_id >= 13)
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price_tier,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels,
        w.capacity AS warehouse_capacity
    FROM base s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        moved_at,
        sku_weight_kg,
        sku_labels,
        MIN(warehouse_capacity) OVER (PARTITION BY operator, unit_price_tier) AS operator_unit_price_tier_min_warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY sku_id, unit_price_tier ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_4,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER (PARTITION BY operator, unit_price_tier), 0), 4) AS sku_weight_kg_share_of_operator_unit_price_tier,
        SUM(sku_weight_kg) OVER (PARTITION BY operator, unit_price_tier) AS operator_unit_price_tier_sum_sku_weight_kg,
        ROUND(PERCENT_RANK() OVER (ORDER BY sku_weight_kg ASC NULLS LAST), 3) AS sku_weight_kg_percent_rank,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY sku_id, unit_price_tier ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_2_prior,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS sku_weight_kg_share_of_move_type_warehouse_id,
        LEAD(qty) OVER (ORDER BY sku_weight_kg ASC NULLS LAST, id) AS next_qty,
        sku_weight_kg - LAG(sku_weight_kg) OVER (ORDER BY warehouse_capacity, id) AS sku_weight_kg_change,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROW_NUMBER() OVER w_all_by_moved_at AS rn,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS warehouse_capacity_share_of_move_type_warehouse_,
        COUNT(*) OVER (PARTITION BY move_type, warehouse_id) AS rows_in_move_type_warehouse_id,
        MAX(sku_weight_kg) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_sku_weight_kg,
        ROUND(AVG(warehouse_capacity) OVER w_move_type_warehouse_id, 2) AS move_type_warehouse_id_avg_warehouse_capacity
    FROM joined_sku
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at ASC, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
      AND SIZE(sku_labels) >= 2
)
-- Final projection
SELECT
    id,
    sku_labels,
    operator_unit_price_tier_min_warehouse_capacity,
    warehouse_capacity_avg_4,
    sku_weight_kg_share_of_operator_unit_price_tier,
    operator_unit_price_tier_sum_sku_weight_kg,
    sku_weight_kg_percent_rank,
    sku_weight_kg_share_of_move_type_warehouse_id,
    next_qty,
    sku_weight_kg_change,
    rn,
    warehouse_capacity_share_of_move_type_warehouse_,
    rows_in_move_type_warehouse_id,
    move_type_warehouse_id_avg_warehouse_capacity,
    sku_weight_kg_change - sku_weight_kg AS sku_weight_kg_change_minus_sku_weight_kg
FROM top_per_group
ORDER BY id DESC NULLS FIRST
