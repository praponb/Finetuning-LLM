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
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        CASE WHEN qty >= 245 THEN 'large' WHEN qty >= 109 THEN 'medium' ELSE 'small' END AS qty_tier
    FROM stock_moves
    WHERE (sku_id <= 12 OR qty >= 99)
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.qty_per_unit_price,
        s.qty_tier,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(moved_at, 3) OVER (ORDER BY warehouse_capacity DESC, id) AS prev3_moved_at,
        COUNT(*) OVER (PARTITION BY move_type, qty_tier) AS rows_in_move_type_qty_tier,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator, warehouse_id ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        ROUND(qty_per_unit_price - AVG(qty_per_unit_price) OVER w_move_type_qty_tier, 2) AS qty_per_unit_price_vs_move_type_qty_tier_avg,
        ROUND(CUME_DIST() OVER w_all_by_moved_at_ties, 3) AS moved_at_cume_dist,
        SIZE(COLLECT_SET(move_type) OVER w_moved_at_month_warehouse_id) AS distinct_move_type_in_moved_at_month_warehouse_i,
        ROUND(AVG(qty_per_unit_price) OVER (PARTITION BY move_type, qty_tier), 2) AS move_type_qty_tier_avg_qty_per_unit_price,
        ROUND(AVG(qty_per_unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id), 2) AS moved_at_month_warehouse_id_avg_qty_per_unit_pri,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY move_type, qty_tier), 2) AS warehouse_capacity_vs_move_type_qty_tier_avg,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type, qty_tier)) AS distinct_operator_in_move_type_qty_tier,
        ROUND(CUME_DIST() OVER w_operator_warehouse_id_by_war_2, 3) AS warehouse_capacity_cume_dist
    FROM enriched
    WINDOW w_move_type_qty_tier AS (PARTITION BY move_type, qty_tier), w_all_by_moved_at_ties AS (ORDER BY moved_at), w_moved_at_month_warehouse_id AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id), w_operator_warehouse_id_by_war_2 AS (PARTITION BY operator, warehouse_id ORDER BY warehouse_capacity DESC)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        qty_per_unit_price,
        warehouse_capacity,
        sku_labels,
        rows_in_move_type_qty_tier,
        last_known_moved_at,
        qty_per_unit_price_vs_move_type_qty_tier_avg,
        moved_at_cume_dist,
        distinct_move_type_in_moved_at_month_warehouse_i,
        move_type_qty_tier_avg_qty_per_unit_price,
        moved_at_month_warehouse_id_avg_qty_per_unit_pri,
        SUM(qty) OVER (PARTITION BY qty_tier ORDER BY distinct_move_type_in_moved_at_month_warehouse_i DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROUND(PERCENT_RANK() OVER w_qty_tier_by_distinct_move_ty_2, 3) AS distinct_move_type_in_moved_at_month_warehouse_i_2,
        NTH_VALUE(moved_at_cume_dist, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_cume_dist,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY distinct_operator_in_move_type_qty_tier, operator), 2) AS warehouse_capacity_vs_distinct_operator_in_move_,
        LEAD(qty) OVER w_all_by_moved_at AS next_qty,
        ROUND(warehouse_capacity_cume_dist - AVG(warehouse_capacity_cume_dist) OVER (PARTITION BY distinct_operator_in_move_type_qty_tier, operator), 2) AS warehouse_capacity_cume_dist_vs_distinct_operato
    FROM with_trends
    WINDOW w_qty_tier_by_distinct_move_ty_2 AS (PARTITION BY qty_tier ORDER BY distinct_move_type_in_moved_at_month_warehouse_i DESC, id), w_all_by_moved_at AS (ORDER BY moved_at ASC, id)
)
SELECT
    id,
    qty,
    moved_at,
    qty_per_unit_price,
    warehouse_capacity,
    rows_in_move_type_qty_tier,
    last_known_moved_at,
    qty_per_unit_price_vs_move_type_qty_tier_avg,
    moved_at_cume_dist,
    distinct_move_type_in_moved_at_month_warehouse_i,
    moved_at_month_warehouse_id_avg_qty_per_unit_pri,
    running_qty,
    distinct_move_type_in_moved_at_month_warehouse_i_2,
    warehouse_capacity_vs_distinct_operator_in_move_,
    next_qty,
    warehouse_capacity_cume_dist_vs_distinct_operato
FROM windowed
ORDER BY next_qty
