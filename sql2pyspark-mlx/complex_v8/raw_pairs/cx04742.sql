WITH base_moves AS (
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
        ABS(qty - 148) AS qty_distance
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
      AND warehouse_id <= 5
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.qty_distance,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        qty_distance,
        sku_category,
        sku_weight_kg,
        sku_unit_cost,
        ROUND(unit_price - AVG(unit_price) OVER w_sku_id, 2) AS unit_price_vs_sku_id_avg,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_category ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(AVG(qty_distance) OVER (PARTITION BY sku_id, YEAR(moved_at)), 2) AS sku_id_moved_at_year_avg_qty_distance,
        SUM(qty_distance) OVER (PARTITION BY sku_category ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_distance_sum_3_prior,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        NTILE(4) OVER (PARTITION BY sku_category, warehouse_id ORDER BY sku_unit_cost, id) AS sku_unit_cost_quartile,
        unit_price - LAG(unit_price) OVER (ORDER BY moved_at DESC, id) AS unit_price_change,
        COUNT(*) OVER (PARTITION BY sku_id, YEAR(moved_at)) AS rows_in_sku_id_moved_at_year,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, YEAR(moved_at))) AS distinct_warehouse_id_in_sku_id_moved_at_year,
        MIN(unit_price) OVER (PARTITION BY sku_id, YEAR(moved_at)) AS sku_id_moved_at_year_min_unit_price,
        MIN(sku_unit_cost) OVER (PARTITION BY sku_category ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_sku_unit_cost_so_far
    FROM combined_details
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM metrics
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_warehouse_id_by_second_unit AS second_unit_price_seq,
        NTILE(3) OVER (PARTITION BY sku_category, distinct_warehouse_id_in_sku_id_moved_at_year ORDER BY qty_distance_sum_3_prior ASC, id) AS qty_distance_sum_3_prior_tercile,
        DENSE_RANK() OVER w_all_by_unit_price_vs_sku_id AS unit_price_vs_sku_id_avg_dense_rank,
        RANK() OVER w_all_by_unit_price_vs_sku_id AS unit_price_vs_sku_id_avg_rank,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type
    FROM labeled
    WINDOW w_warehouse_id_by_second_unit AS (PARTITION BY warehouse_id ORDER BY second_unit_price ASC, id), w_all_by_unit_price_vs_sku_id AS (ORDER BY unit_price_vs_sku_id_avg)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    qty_distance,
    sku_category,
    sku_unit_cost,
    unit_price_vs_sku_id_avg,
    second_unit_price,
    sku_id_moved_at_year_avg_qty_distance,
    qty_distance_sum_3_prior,
    sku_unit_cost_quartile,
    unit_price_change,
    rows_in_sku_id_moved_at_year,
    moved_at_label,
    second_unit_price_seq,
    qty_distance_sum_3_prior_tercile,
    unit_price_vs_sku_id_avg_dense_rank,
    unit_price_vs_sku_id_avg_rank,
    rows_in_move_type
FROM with_windows
ORDER BY sku_unit_cost_quartile NULLS LAST, qty_distance_sum_3_prior_tercile NULLS LAST, id
LIMIT 10
