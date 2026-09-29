WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
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
    WHERE unit_price >= 189.4
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        sku_is_active,
        sku_labels,
        sku_unit_cost,
        SUM(sku_unit_cost) OVER w_operator AS operator_sum_sku_unit_cost,
        COUNT(*) OVER (PARTITION BY sku_is_active, warehouse_id) AS rows_in_sku_is_active_warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY sku_unit_cost, id) AS sku_unit_cost_seq,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SUM(sku_unit_cost) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sku_unit_cost_sum_1_prior,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER (PARTITION BY operator), 0), 4) AS sku_unit_cost_share_of_operator,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY sku_is_active, warehouse_id), 2) AS sku_unit_cost_vs_sku_is_active_warehouse_id_avg,
        MAX(sku_unit_cost) OVER w_sku_is_active_warehouse_id AS sku_is_active_warehouse_id_max_sku_unit_cost,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_is_active, warehouse_id)) AS distinct_operator_in_sku_is_active_warehouse_id
    FROM combined_details_sku
    WINDOW w_operator AS (PARTITION BY operator), w_sku_is_active_warehouse_id AS (PARTITION BY sku_is_active, warehouse_id)
)
SELECT
    id,
    sku_id,
    operator,
    sku_is_active,
    sku_labels,
    rows_in_sku_is_active_warehouse_id,
    sku_unit_cost_seq,
    sku_unit_cost_sum_1_prior,
    sku_unit_cost_share_of_operator,
    sku_unit_cost_vs_sku_is_active_warehouse_id_avg,
    sku_is_active_warehouse_id_max_sku_unit_cost,
    FLOOR(sku_unit_cost / 50) * 50 AS sku_unit_cost_bucket
FROM metrics
WHERE warehouse_id <= 3
ORDER BY sku_unit_cost_bucket DESC
