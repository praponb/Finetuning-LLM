WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        CASE WHEN unit_price >= 330.0 THEN 'A' WHEN unit_price >= 112.8 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE unit_price > 130.6
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM prepared s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(sku_weight_kg, 3) OVER (PARTITION BY warehouse_id ORDER BY warehouse_capacity DESC, id) AS prev3_sku_weight_kg,
        RANK() OVER w_all_by_warehouse_capacity AS warehouse_capacity_rank,
        warehouse_capacity - LAG(warehouse_capacity) OVER w_unit_price_tier_move_type_by AS warehouse_capacity_change,
        DENSE_RANK() OVER (PARTITION BY unit_price_tier, move_type ORDER BY unit_price DESC) AS unit_price_dense_rank,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY move_type), 2) AS move_type_avg_sku_weight_kg,
        LEAD(unit_price) OVER (ORDER BY warehouse_capacity, id) AS next_unit_price,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type)) AS distinct_operator_in_move_type,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type), 2) AS qty_vs_move_type_avg,
        LAST_VALUE(moved_at) OVER (PARTITION BY warehouse_id ORDER BY warehouse_capacity NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        DENSE_RANK() OVER w_warehouse_id_sku_category_by AS qty_dense_rank
    FROM with_parent
    WINDOW w_all_by_warehouse_capacity AS (ORDER BY warehouse_capacity, id), w_unit_price_tier_move_type_by AS (PARTITION BY unit_price_tier, move_type ORDER BY unit_price DESC, id), w_warehouse_id_sku_category_by AS (PARTITION BY warehouse_id, sku_category ORDER BY qty ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY unit_price_dense_rank ORDER BY moved_at DESC NULLS FIRST, id) AS seq_no,
        DATEDIFF(moved_at_date, LAG(moved_at_date) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_date) ORDER BY unit_price_dense_rank, id)) AS days_since_prev_moved_at_date,
        NTH_VALUE(moved_at_date, 2) OVER (ORDER BY warehouse_capacity_change ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_date
    FROM with_trends
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price_tier,
    warehouse_capacity,
    sku_category,
    sku_weight_kg,
    prev3_sku_weight_kg,
    warehouse_capacity_rank,
    warehouse_capacity_change,
    move_type_avg_sku_weight_kg,
    next_unit_price,
    distinct_operator_in_move_type,
    last_moved_at,
    qty_dense_rank,
    seq_no,
    days_since_prev_moved_at_date,
    second_moved_at_date,
    DATE_ADD(second_moved_at_date, 90) AS second_moved_at_date_plus_90d
FROM metrics
WHERE warehouse_capacity >= 2614
ORDER BY next_unit_price
