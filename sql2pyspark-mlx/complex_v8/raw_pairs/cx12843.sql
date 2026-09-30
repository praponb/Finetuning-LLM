WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE (operator = 'op_chan' OR warehouse_id IS NOT NULL)
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY move_type, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        ROW_NUMBER() OVER (PARTITION BY move_type, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id) AS rn,
        MIN(unit_price) OVER (PARTITION BY move_type, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY sku_id, TO_DATE(moved_at) ORDER BY sku_weight_kg, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS sku_weight_kg_avg_4,
        NTILE(4) OVER (PARTITION BY sku_is_active, MONTH(moved_at) ORDER BY unit_price, id) AS unit_price_quartile,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_is_active, sku_id)) AS distinct_move_type_in_sku_is_active_sku_id,
        MIN(unit_price) OVER (PARTITION BY sku_is_active) AS sku_is_active_min_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_is_active)) AS distinct_warehouse_id_in_sku_is_active,
        ROUND(AVG(sku_weight_kg) OVER (ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS sku_weight_kg_avg_2,
        SUM(sku_weight_kg) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sku_weight_kg_sum_3,
        LAG(moved_at) OVER (ORDER BY moved_at DESC, id) AS prev_moved_at
    FROM enriched_sku
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CASE WHEN move_type IN ('OUTBOUND', 'INBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND((UNIX_TIMESTAMP(prev_moved_at) - UNIX_TIMESTAMP(moved_at)) / 60, 1) AS minutes_moved_at_to_prev_moved_at
    FROM with_trends
    WHERE sku_is_active_min_unit_price > 2250.6
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    sku_labels,
    sku_weight_kg,
    sku_is_active,
    second_sku_weight_kg,
    min_unit_price_so_far,
    sku_weight_kg_avg_4,
    unit_price_quartile,
    distinct_move_type_in_sku_is_active_sku_id,
    sku_is_active_min_unit_price,
    distinct_warehouse_id_in_sku_is_active,
    sku_weight_kg_avg_2,
    sku_weight_kg_sum_3,
    prev_moved_at,
    move_type_or_unknown,
    minutes_moved_at_to_prev_moved_at,
    CONCAT(move_type_group, '#', CAST(warehouse_id AS STRING)) AS move_type_group_ref,
    ARRAY_JOIN(ARRAY_SORT(sku_labels), ',') AS sku_labels_text
FROM annotated
ORDER BY sku_weight_kg DESC NULLS FIRST, id
LIMIT 10
