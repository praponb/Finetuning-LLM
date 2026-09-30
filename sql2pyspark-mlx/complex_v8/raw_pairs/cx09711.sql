WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE unit_price <= 290.4
      AND warehouse_id <= 2
),
joined AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        w.city AS warehouse_city
    FROM recent s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        sku_weight_kg,
        sku_labels,
        warehouse_city,
        RANK() OVER (ORDER BY qty) AS qty_rank,
        LEAD(moved_at) OVER (ORDER BY qty, id) AS next_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id, 0), 4) AS unit_price_share_of_sku_id,
        RANK() OVER (PARTITION BY operator ORDER BY qty DESC) AS qty_rank_2,
        SUM(sku_weight_kg) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_weight_kg,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        DENSE_RANK() OVER w_warehouse_id_by_moved_at_tie AS moved_at_dense_rank,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY warehouse_city, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        LAST_VALUE(moved_at) OVER (ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_city), 0), 4) AS unit_price_share_of_sku_id_warehouse_city,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city,
        SUM(qty) OVER (PARTITION BY warehouse_city, sku_id ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7,
        MAX(sku_weight_kg) OVER (PARTITION BY warehouse_city) AS warehouse_city_max_sku_weight_kg,
        DENSE_RANK() OVER (ORDER BY unit_price DESC NULLS FIRST, id) AS unit_price_dense_rank,
        MAX(unit_price) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far
    FROM joined
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_warehouse_id_by_moved_at_tie AS (PARTITION BY warehouse_id ORDER BY moved_at DESC)
),
totals AS (
    -- Summarise per group
    SELECT
        moved_at_dense_rank,
        DATE_FORMAT(next_moved_at, 'yyyy-MM') AS next_moved_at_ym,
        SUM(qty_sum_7) AS total_qty_sum_7,
        SUM(CASE WHEN operator IS NOT NULL THEN 1 ELSE 0 END) AS n_operator_is_not_null,
        PERCENTILE_APPROX(sku_weight_kg, 0.5) AS median_sku_weight_kg,
        MAX(moved_at) AS last_moved_at_2,
        COUNT(*) AS row_count
    FROM metrics
    WHERE sku_weight_kg >= 13.6
    GROUP BY moved_at_dense_rank, DATE_FORMAT(next_moved_at, 'yyyy-MM')
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        total_qty_sum_7 - LAG(total_qty_sum_7) OVER (PARTITION BY n_operator_is_not_null, row_count ORDER BY row_count, moved_at_dense_rank, next_moved_at_ym) AS total_qty_sum_7_change,
        ROUND(CUME_DIST() OVER (PARTITION BY n_operator_is_not_null, row_count ORDER BY row_count, moved_at_dense_rank, next_moved_at_ym), 3) AS row_count_cume_dist,
        ROUND(AVG(row_count) OVER (PARTITION BY moved_at_dense_rank, row_count ORDER BY n_operator_is_not_null, moved_at_dense_rank, next_moved_at_ym ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS row_count_avg_3_prior,
        MAX(median_sku_weight_kg) OVER (PARTITION BY moved_at_dense_rank, row_count ORDER BY n_operator_is_not_null, moved_at_dense_rank, next_moved_at_ym ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS median_sku_weight_kg_max_3_prior
    FROM totals
)
-- Final projection
SELECT
    moved_at_dense_rank,
    next_moved_at_ym,
    total_qty_sum_7,
    median_sku_weight_kg,
    last_moved_at_2,
    row_count,
    total_qty_sum_7_change,
    row_count_cume_dist,
    row_count_avg_3_prior,
    median_sku_weight_kg_max_3_prior,
    COALESCE(row_count_cume_dist, 0.0) AS row_count_cume_dist_filled,
    DATE_TRUNC('MONTH', last_moved_at_2) AS last_moved_at_2_month
FROM metrics_2
WHERE next_moved_at_ym IS NOT NULL
ORDER BY row_count_cume_dist DESC
