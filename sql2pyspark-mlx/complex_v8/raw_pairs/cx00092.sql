WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND unit_price > 152.7
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM scoped_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY warehouse_opened_on ASC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        MAX(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        NTILE(10) OVER w_move_type_warehouse_id_by_wa AS warehouse_opened_on_decile,
        MAX(qty) OVER (ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far_2,
        ROUND(AVG(qty) OVER w_sku_id, 2) AS sku_id_avg_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)), 0), 4) AS qty_share_of_moved_at_month,
        ROUND(AVG(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)), 2) AS moved_at_month_avg_qty,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at ASC) AS moved_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY qty), 3) AS qty_cume_dist,
        RANK() OVER w_all_by_warehouse_opened_on_t AS warehouse_opened_on_rank,
        SUM(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior,
        FIRST_VALUE(warehouse_opened_on) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_warehouse_opened_on,
        MAX(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior
    FROM combined_details
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_move_type_warehouse_id_by_wa AS (PARTITION BY move_type, warehouse_id ORDER BY warehouse_opened_on ASC, id), w_all_by_warehouse_opened_on_t AS (ORDER BY warehouse_opened_on ASC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        qty_sum_3_prior - LAG(qty_sum_3_prior) OVER w_all_by_qty_share_of_moved_at AS qty_sum_3_prior_change,
        ROUND(CUME_DIST() OVER (ORDER BY qty_share_of_moved_at_month), 3) AS qty_share_of_moved_at_month_cume_dist,
        MAX(qty_sum_3_prior) OVER (ORDER BY qty_share_of_moved_at_month DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior_max_2_prior,
        COUNT(*) OVER w_warehouse_opened_on_month AS rows_in_warehouse_opened_on_month,
        warehouse_opened_on_decile - LAG(warehouse_opened_on_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', first_warehouse_opened_on), moved_at_rank ORDER BY first_warehouse_opened_on, id) AS warehouse_opened_on_decile_change,
        FIRST_VALUE(sku_id_avg_qty) OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY sku_id_avg_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_id_avg_qty
    FROM with_trends
    WINDOW w_all_by_qty_share_of_moved_at AS (ORDER BY qty_share_of_moved_at_month DESC NULLS FIRST, id), w_warehouse_opened_on_month AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on))
),
totals AS (
    -- Summarise per group
    SELECT
        sku_id,
        CASE WHEN first_sku_id_avg_qty >= 300.4 THEN 'large' WHEN first_sku_id_avg_qty >= 200.6 THEN 'medium' ELSE 'small' END AS first_sku_id_avg_qty_tier,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        DATEDIFF(MAX(warehouse_opened_on), MIN(warehouse_opened_on)) AS warehouse_opened_on_span_days,
        ROUND(SUM(qty_sum_3_prior_change) / NULLIF(SUM(qty_sum_3_prior_max_2_prior), 0), 3) AS qty_sum_3_prior_change_to_qty_sum_3_prior_max_2_
    FROM with_windows
    GROUP BY sku_id, CASE WHEN first_sku_id_avg_qty >= 300.4 THEN 'large' WHEN first_sku_id_avg_qty >= 200.6 THEN 'medium' ELSE 'small' END, DATE_FORMAT(moved_at, 'yyyy-MM')
)
SELECT
    sku_id,
    first_sku_id_avg_qty_tier,
    moved_at_ym,
    warehouse_opened_on_span_days,
    qty_sum_3_prior_change_to_qty_sum_3_prior_max_2_,
    SUBSTRING(TRIM(moved_at_ym), 1, 2) AS moved_at_ym_prefix,
    SUBSTRING(TRIM(moved_at_ym), 1, 2) AS moved_at_ym_prefix_2
FROM totals
