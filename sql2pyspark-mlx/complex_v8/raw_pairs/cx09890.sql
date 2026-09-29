WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE sku_id <= 17
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.operator_ref,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_sku_id_warehouse_id_by_moved, 3) AS moved_at_percent_rank,
        NTILE(3) OVER w_sku_id_moved_at_month_by_mov AS moved_at_tercile,
        SUM(sku_unit_cost) OVER (PARTITION BY sku_id) AS sku_id_sum_sku_unit_cost,
        ROUND(CUME_DIST() OVER w_warehouse_id_by_qty_ties, 3) AS qty_cume_dist,
        LAG(unit_price, 2, 0) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id) AS prev2_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        ROUND(CUME_DIST() OVER w_sku_id_warehouse_id_by_moved_2, 3) AS moved_at_cume_dist,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, MONTH(moved_at)), 2) AS qty_vs_warehouse_id_moved_at_month_avg,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_3_prior,
        LEAD(qty) OVER w_sku_id_by_moved_at AS next_qty,
        MAX(unit_price) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER (PARTITION BY warehouse_id, MONTH(moved_at)), 0), 4) AS sku_unit_cost_share_of_warehouse_id_moved_at_mon,
        MAX(sku_unit_cost) OVER w_sku_id AS sku_id_max_sku_unit_cost
    FROM enriched_sku
    WINDOW w_sku_id_warehouse_id_by_moved AS (PARTITION BY sku_id, warehouse_id ORDER BY moved_at), w_sku_id_moved_at_month_by_mov AS (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at DESC, id), w_warehouse_id_by_qty_ties AS (PARTITION BY warehouse_id ORDER BY qty), w_sku_id_warehouse_id_by_moved_2 AS (PARTITION BY sku_id, warehouse_id ORDER BY moved_at, id), w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at DESC, id), w_sku_id AS (PARTITION BY sku_id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(moved_at_tercile, 0) AS moved_at_tercile_filled,
        CASE WHEN unit_price >= 299.1 THEN 'HIGH' WHEN unit_price >= 189.4 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        COALESCE(moved_at_cume_dist, 0.0) AS moved_at_cume_dist_filled
    FROM metrics
    WHERE sku_id_sum_sku_unit_cost < 2800.3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(moved_at_tercile_filled) OVER (ORDER BY moved_at_tercile_filled, id) AS prev_moved_at_tercile_filled,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY moved_at_tercile_filled)) AS distinct_warehouse_id_in_moved_at_tercile_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at_tercile_filled), 3) AS moved_at_tercile_filled_percent_rank,
        RANK() OVER w_all_by_sku_id_max_sku_unit_c AS sku_id_max_sku_unit_cost_rank,
        ROUND(AVG(sku_id_sum_sku_unit_cost) OVER (ORDER BY moved_at_tercile_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS sku_id_sum_sku_unit_cost_avg_2_prior,
        ROUND(moved_at_percent_rank / NULLIF(SUM(moved_at_percent_rank) OVER (PARTITION BY moved_at_tercile_filled), 0), 4) AS moved_at_percent_rank_share_of_moved_at_tercile_
    FROM scored
    WINDOW w_all_by_moved_at_tercile_fill_2 AS (ORDER BY moved_at_tercile_filled), w_all_by_sku_id_max_sku_unit_c AS (ORDER BY sku_id_max_sku_unit_cost DESC)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    sku_unit_cost,
    moved_at_tercile,
    sku_id_sum_sku_unit_cost,
    qty_cume_dist,
    rows_in_sku_id,
    qty_vs_warehouse_id_moved_at_month_avg,
    sku_unit_cost_avg_3_prior,
    next_qty,
    sku_unit_cost_share_of_warehouse_id_moved_at_mon,
    sku_id_max_sku_unit_cost,
    moved_at_tercile_filled,
    moved_at_cume_dist_filled,
    prev_moved_at_tercile_filled,
    distinct_warehouse_id_in_moved_at_tercile_filled,
    moved_at_tercile_filled_percent_rank,
    sku_id_max_sku_unit_cost_rank,
    sku_id_sum_sku_unit_cost_avg_2_prior,
    moved_at_percent_rank_share_of_moved_at_tercile_,
    DAYOFWEEK(moved_at) AS moved_at_dayofweek
FROM with_trends
ORDER BY qty_cume_dist DESC, sku_unit_cost_share_of_warehouse_id_moved_at_mon DESC
