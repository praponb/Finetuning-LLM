WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        CASE WHEN move_type IN ('INBOUND', 'TRANSFER') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.days_since_moved_at,
        s.move_type_group,
        w.wh_code AS warehouse_wh_code,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels
    FROM recent_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY move_type_group, operator ORDER BY sku_weight_kg NULLS LAST, id) AS sku_weight_kg_tercile,
        SUM(days_since_moved_at) OVER (PARTITION BY operator ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_moved_at_sum_2,
        LAST_VALUE(days_since_moved_at) OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_moved_at,
        DENSE_RANK() OVER (PARTITION BY operator, sku_id ORDER BY unit_price DESC, id) AS unit_price_dense_rank,
        LAG(days_since_moved_at, 3, 0) OVER (PARTITION BY operator ORDER BY unit_price DESC, id) AS prev3_days_since_moved_at,
        MAX(days_since_moved_at) OVER (PARTITION BY operator) AS operator_max_days_since_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER (PARTITION BY sku_id), 0), 4) AS days_since_moved_at_share_of_sku_id
    FROM with_details
    WINDOW w_operator AS (PARTITION BY operator)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(sku_weight_kg) OVER (PARTITION BY warehouse_id ORDER BY sku_weight_kg_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sku_weight_kg,
        MIN(days_since_moved_at) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_min_days_since_moved_at,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER (PARTITION BY sku_id), 0), 4) AS days_since_moved_at_share_of_sku_id_2,
        SUM(last_days_since_moved_at) OVER (ORDER BY days_since_moved_at_share_of_sku_id, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last_days_since_moved_at_sum_3_prior,
        ROUND(unit_price_dense_rank - AVG(unit_price_dense_rank) OVER (PARTITION BY sku_id), 2) AS unit_price_dense_rank_vs_sku_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type_group ORDER BY operator_max_days_since_moved_at DESC, id), 3) AS operator_max_days_since_moved_at_percent_rank,
        ROW_NUMBER() OVER w_warehouse_id_by_sku_weight_k_2 AS rn
    FROM metrics
    WINDOW w_warehouse_id_by_sku_weight_k_2 AS (PARTITION BY warehouse_id ORDER BY sku_weight_kg_tercile DESC, id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    unit_price,
    days_since_moved_at,
    move_type_group,
    warehouse_wh_code,
    sku_labels,
    sku_weight_kg_tercile,
    days_since_moved_at_sum_2,
    last_days_since_moved_at,
    unit_price_dense_rank,
    operator_max_days_since_moved_at,
    distinct_warehouse_id_in_sku_id,
    operator_warehouse_id_min_days_since_moved_at,
    days_since_moved_at_share_of_sku_id_2,
    last_days_since_moved_at_sum_3_prior,
    unit_price_dense_rank_vs_sku_id_avg,
    operator_max_days_since_moved_at_percent_rank,
    rn,
    COALESCE(rn, 0) AS rn_filled,
    COALESCE(sku_weight_kg, 0.0) AS sku_weight_kg_filled
FROM sequenced
ORDER BY unit_price_dense_rank_vs_sku_id_avg, id
LIMIT 5
