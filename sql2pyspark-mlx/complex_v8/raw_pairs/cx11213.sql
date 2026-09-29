WITH recent AS (
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
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator <> 'op_chan'
      AND warehouse_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        NTILE(5) OVER w_sku_category_by_sku_weight_k AS sku_weight_kg_quintile,
        DENSE_RANK() OVER (ORDER BY sku_weight_kg DESC) AS sku_weight_kg_dense_rank,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id), 2) AS sku_weight_kg_vs_warehouse_id_avg,
        LEAD(sku_unit_cost) OVER (ORDER BY sku_weight_kg DESC, id) AS next_sku_unit_cost,
        SIZE(COLLECT_SET(sku_category) OVER (PARTITION BY move_type)) AS distinct_sku_category_in_move_type,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY move_type), 2) AS move_type_avg_sku_unit_cost,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY move_type), 2) AS move_type_avg_sku_weight_kg,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER (PARTITION BY move_type), 0), 4) AS sku_weight_kg_share_of_move_type,
        NTILE(3) OVER (ORDER BY sku_weight_kg DESC, id) AS sku_weight_kg_tercile,
        ROUND(CUME_DIST() OVER w_warehouse_id_by_sku_weight_k, 3) AS sku_weight_kg_cume_dist,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id), 2) AS sku_unit_cost_vs_warehouse_id_avg
    FROM joined
    WINDOW w_sku_category_by_sku_weight_k AS (PARTITION BY sku_category ORDER BY sku_weight_kg, id), w_warehouse_id_by_sku_weight_k AS (PARTITION BY warehouse_id ORDER BY sku_weight_kg DESC)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE sku_weight_kg_dense_rank = 1
)
-- Final projection
SELECT
    id,
    sku_id,
    operator,
    sku_unit_cost,
    sku_category,
    sku_weight_kg,
    rows_in_move_type,
    sku_weight_kg_quintile,
    sku_weight_kg_dense_rank,
    next_sku_unit_cost,
    move_type_avg_sku_weight_kg,
    sku_weight_kg_share_of_move_type,
    sku_weight_kg_cume_dist,
    sku_unit_cost_vs_warehouse_id_avg,
    ROUND(sku_weight_kg_tercile / NULLIF(sku_unit_cost, 0), 3) AS sku_weight_kg_tercile_per_sku_unit_cost,
    ABS(sku_weight_kg_quintile - 4) AS sku_weight_kg_quintile_distance
FROM top_per_group
WHERE distinct_sku_category_in_move_type <= 8
ORDER BY sku_unit_cost, id
LIMIT 5
