WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        CASE WHEN qty >= 314 THEN 'A' WHEN qty >= 111 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE (operator NOT IN ('op_eve', 'op_dew') OR moved_at >= TIMESTAMP '2025-01-31 08:00:00')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.qty_tier,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY move_type ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, sku_is_active)) AS distinct_operator_in_warehouse_id_sku_is_active,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_is_active), 2) AS unit_price_vs_warehouse_id_sku_is_active_avg,
        SUM(sku_unit_cost) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_unit_cost,
        ROUND(AVG(qty) OVER (PARTITION BY sku_is_active ORDER BY qty NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        SIZE(COLLECT_SET(qty_tier) OVER (PARTITION BY move_type)) AS distinct_qty_tier_in_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        ROUND(PERCENT_RANK() OVER w_operator_sku_id_by_sku_unit_2, 3) AS sku_unit_cost_percent_rank
    FROM with_parent
    WINDOW w_operator_sku_id_by_sku_unit_2 AS (PARTITION BY operator, sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    qty_tier,
    sku_labels,
    sku_is_active,
    sku_unit_cost,
    running_unit_price,
    distinct_operator_in_warehouse_id_sku_is_active,
    unit_price_vs_warehouse_id_sku_is_active_avg,
    running_sku_unit_cost,
    unit_price_avg_1_prior,
    distinct_qty_tier_in_move_type,
    COALESCE(unit_price_avg_6_prior, 0.0) AS unit_price_avg_6_prior_filled,
    UPPER(TRIM(move_type)) AS move_type_clean
FROM with_windows
