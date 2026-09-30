WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE qty < 372
      AND unit_price BETWEEN 73.0 AND 330.9
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.weight_kg < 15.2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id), 2) AS sku_unit_cost_vs_warehouse_id_avg,
        NTH_VALUE(sku_weight_kg, 2) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        sku_weight_kg - LAG(sku_weight_kg) OVER (ORDER BY qty DESC, id) AS sku_weight_kg_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_is_active ORDER BY sku_weight_kg), 3) AS sku_weight_kg_percent_rank,
        MAX(sku_weight_kg) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_sku_weight_kg,
        SUM(sku_unit_cost) OVER w_warehouse_id AS warehouse_id_sum_sku_unit_cost,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_id, sku_is_active ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg_2,
        NTILE(3) OVER (ORDER BY qty DESC, id) AS qty_tercile,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id)) AS distinct_operator_in_warehouse_id,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id) AS distinct_sku_id_in_warehouse_id,
        RANK() OVER (PARTITION BY sku_is_active ORDER BY sku_weight_kg) AS sku_weight_kg_rank,
        MIN(sku_unit_cost) OVER w_warehouse_id AS warehouse_id_min_sku_unit_cost,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_sku_weight_kg
    FROM enriched_sku
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        second_sku_weight_kg_2 - sku_weight_kg AS second_sku_weight_kg_2_minus_sku_weight_kg,
        ROUND(second_sku_weight_kg_2 * 0.25, 2) AS second_sku_weight_kg_2_adj
    FROM with_windows
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    sku_weight_kg,
    sku_is_active,
    second_sku_weight_kg,
    sku_weight_kg_percent_rank,
    warehouse_id_max_sku_weight_kg,
    warehouse_id_sum_sku_unit_cost,
    qty_tercile,
    distinct_sku_id_in_warehouse_id,
    sku_weight_kg_rank,
    warehouse_id_min_sku_unit_cost,
    warehouse_id_avg_sku_weight_kg,
    second_sku_weight_kg_2_minus_sku_weight_kg,
    COALESCE(second_sku_weight_kg, 0.0) AS second_sku_weight_kg_filled
FROM annotated
WHERE operator = 'op_dew'
ORDER BY sku_weight_kg_percent_rank DESC NULLS FIRST
