WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE qty BETWEEN 124 AND 422
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(unit_price_minus_qty, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_minus_qty,
        ROUND(AVG(unit_price_minus_qty) OVER w_sku_id_warehouse_id, 2) AS sku_id_warehouse_id_avg_unit_price_minus_qty,
        ROUND(unit_price_minus_qty - AVG(unit_price_minus_qty) OVER w_sku_id_warehouse_id, 2) AS unit_price_minus_qty_vs_sku_id_warehouse_id_avg,
        DENSE_RANK() OVER w_warehouse_id_moved_at_month AS unit_price_minus_qty_dense_rank,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY unit_price_minus_qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        NTILE(4) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty ASC, id) AS qty_quartile,
        SUM(unit_price_minus_qty) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price_minus_qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_minus_qty_sum_6_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_share_of_warehouse_id,
        SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_sum_unit_price,
        LAG(unit_price, 2, 0) OVER w_moved_at_month_by_qty AS prev2_unit_price,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at DESC) AS moved_at_dense_rank,
        MAX(unit_price) OVER w_sku_id_warehouse_id AS sku_id_warehouse_id_max_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(moved_at) ORDER BY qty DESC, id), 3) AS qty_percent_rank,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id) AS distinct_sku_id_in_warehouse_id
    FROM scoped
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_warehouse_id_moved_at_month AS (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price_minus_qty), w_moved_at_month_by_qty AS (PARTITION BY MONTH(moved_at) ORDER BY qty DESC, id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        unit_price_minus_qty,
        second_unit_price_minus_qty,
        sku_id_warehouse_id_avg_unit_price_minus_qty,
        unit_price_minus_qty_vs_sku_id_warehouse_id_avg,
        unit_price_minus_qty_dense_rank,
        minutes_since_prev_moved_at,
        unit_price_minus_qty_sum_6_prior,
        sku_id_warehouse_id_sum_unit_price,
        prev2_unit_price,
        moved_at_dense_rank,
        sku_id_warehouse_id_max_unit_price,
        qty_percent_rank,
        second_moved_at,
        distinct_sku_id_in_warehouse_id,
        ROUND(AVG(qty_percent_rank) OVER (PARTITION BY qty_quartile ORDER BY qty_percent_rank, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_percent_rank_avg_1_prior,
        LAG(unit_price) OVER w_unit_price_minus_qty_dense_r AS prev_unit_price,
        ROUND(AVG(qty_percent_rank) OVER (PARTITION BY qty_quartile ORDER BY qty_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_percent_rank_avg_3,
        COUNT(*) OVER w_moved_at_dense_rank_sku_id AS rows_in_moved_at_dense_rank_sku_id,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY moved_at_dense_rank, sku_id)) AS distinct_warehouse_id_in_moved_at_dense_rank_sku,
        ROUND(CUME_DIST() OVER w_qty_quartile_by_qty_percent_3, 3) AS qty_percent_rank_cume_dist,
        LAG(first_known_qty, 3) OVER (PARTITION BY qty_quartile, DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price_minus_qty_dense_rank DESC NULLS FIRST, id) AS prev3_first_known_qty
    FROM ranked
    WINDOW w_unit_price_minus_qty_dense_r AS (PARTITION BY unit_price_minus_qty_dense_rank, distinct_sku_id_in_warehouse_id ORDER BY sku_id_warehouse_id_sum_unit_price ASC, id), w_moved_at_dense_rank_sku_id AS (PARTITION BY moved_at_dense_rank, sku_id), w_qty_quartile_by_qty_percent_3 AS (PARTITION BY qty_quartile ORDER BY qty_percent_rank, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        MIN(second_moved_at) AS first_second_moved_at,
        COUNT(*) AS row_count
    FROM ranked_2
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        moved_at_month,
        row_count,
        MAX(row_count) OVER (ORDER BY row_count DESC, moved_at_month ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_count_max_2_prior,
        SUM(row_count) OVER (ORDER BY row_count DESC, moved_at_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_count_sum_3_prior,
        LAST_VALUE(moved_at_month) OVER (ORDER BY row_count DESC, moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_month,
        ROUND(CUME_DIST() OVER (ORDER BY row_count DESC), 3) AS row_count_cume_dist
    FROM summary
)
-- Final projection
SELECT
    moved_at_month,
    row_count,
    row_count_sum_3_prior,
    row_count_cume_dist,
    DATE_TRUNC('QUARTER', last_moved_at_month) AS last_moved_at_month_quarter,
    TO_DATE(last_moved_at_month) AS last_moved_at_month_date
FROM metrics
ORDER BY moved_at_month
