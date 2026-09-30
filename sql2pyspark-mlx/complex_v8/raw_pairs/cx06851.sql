WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_ref,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        operator_ref,
        sku_weight_kg,
        NTILE(4) OVER (PARTITION BY MONTH(moved_at), sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id) AS sku_unit_cost_quartile,
        SUM(unit_price) OVER (PARTITION BY move_type, operator) AS move_type_operator_sum_unit_price,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id, TO_DATE(moved_at) ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(moved_at), sku_id ORDER BY sku_unit_cost DESC NULLS FIRST), 3) AS sku_unit_cost_cume_dist,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, operator), 2) AS unit_price_vs_move_type_operator_avg,
        MAX(unit_price) OVER w_move_type_operator AS move_type_operator_max_unit_price,
        LEAD(sku_unit_cost, 2) OVER (ORDER BY moved_at, id) AS next2_sku_unit_cost,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id)) AS distinct_move_type_in_warehouse_id,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id), 2) AS sku_unit_cost_vs_warehouse_id_avg,
        LAST_VALUE(sku_weight_kg) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_weight_kg,
        MAX(sku_unit_cost) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sku_unit_cost_max_4
    FROM combined_details_sku
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(last_known_sku_weight_kg * 0.25, 2) AS last_known_sku_weight_kg_adj,
        HOUR(second_moved_at) AS second_moved_at_hour
    FROM sequenced
)
SELECT
    id,
    warehouse_id,
    moved_at,
    sku_weight_kg,
    sku_unit_cost_quartile,
    move_type_operator_sum_unit_price,
    second_moved_at,
    sku_unit_cost_cume_dist,
    unit_price_vs_move_type_operator_avg,
    move_type_operator_max_unit_price,
    next2_sku_unit_cost,
    sku_unit_cost_vs_warehouse_id_avg,
    last_known_sku_weight_kg,
    sku_unit_cost_max_4,
    last_known_sku_weight_kg_adj,
    second_moved_at_hour,
    CASE WHEN operator IN ('op_chan', 'op_bee') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group
FROM derived
ORDER BY unit_price_vs_move_type_operator_avg, warehouse_id DESC
