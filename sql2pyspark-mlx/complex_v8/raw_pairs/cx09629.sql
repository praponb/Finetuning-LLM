WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id <= 19
      AND warehouse_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        qty,
        unit_price,
        days_since_moved_at,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_share_of_warehouse_id,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_moved_at_avg_7,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_2_prior,
        SUM(days_since_moved_at) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_moved_at_sum_4,
        MAX(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        MAX(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_max_2
    FROM recent_moves
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE days_since_moved_at_avg_7 > (SELECT AVG(days_since_moved_at_avg_7) FROM with_windows)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        unit_price,
        days_since_moved_at,
        qty_share_of_warehouse_id,
        days_since_moved_at_avg_7,
        unit_price_avg_2_prior,
        days_since_moved_at_sum_4,
        max_qty_so_far,
        qty_max_2,
        ROUND(unit_price_avg_2_prior / NULLIF(qty_max_2, 0), 3) AS unit_price_avg_2_prior_per_qty_max_2,
        qty - unit_price_avg_2_prior AS qty_minus_unit_price_avg_2_prior,
        FLOOR(qty_max_2 / 250) * 250 AS qty_max_2_bucket
    FROM with_activity
    WHERE unit_price >= 215.4
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        unit_price,
        days_since_moved_at,
        days_since_moved_at_avg_7,
        unit_price_avg_2_prior,
        days_since_moved_at_sum_4,
        qty_max_2,
        unit_price_avg_2_prior_per_qty_max_2,
        qty_minus_unit_price_avg_2_prior,
        qty_max_2_bucket,
        LAST_VALUE(qty_max_2_bucket) IGNORE NULLS OVER (ORDER BY days_since_moved_at_sum_4, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_max_2_bucket,
        LEAD(max_qty_so_far, 2) OVER (ORDER BY unit_price_avg_2_prior, id) AS next2_max_qty_so_far,
        ROUND(AVG(max_qty_so_far) OVER (ORDER BY unit_price_avg_2_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS max_qty_so_far_avg_7,
        LEAD(days_since_moved_at_avg_7) OVER (ORDER BY days_since_moved_at_sum_4, id) AS next_days_since_moved_at_avg_7,
        LAG(max_qty_so_far) OVER w_all_by_unit_price_avg_2_prio AS prev_max_qty_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY days_since_moved_at_sum_4), 3) AS days_since_moved_at_sum_4_cume_dist
    FROM scored
    WINDOW w_all_by_unit_price_avg_2_prio AS (ORDER BY unit_price_avg_2_prior, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        unit_price,
        days_since_moved_at,
        days_since_moved_at_avg_7,
        unit_price_avg_2_prior,
        days_since_moved_at_sum_4,
        unit_price_avg_2_prior_per_qty_max_2,
        qty_max_2_bucket,
        next2_max_qty_so_far,
        max_qty_so_far_avg_7,
        next_days_since_moved_at_avg_7,
        prev_max_qty_so_far,
        ROUND(PERCENT_RANK() OVER w_all_by_max_qty_so_far_avg_7, 3) AS max_qty_so_far_avg_7_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY max_qty_so_far_avg_7 ASC), 3) AS max_qty_so_far_avg_7_cume_dist,
        ROUND(AVG(qty_max_2_bucket) OVER (ORDER BY max_qty_so_far_avg_7 ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_max_2_bucket_avg_2_prior,
        qty_max_2_bucket - LAG(qty_max_2_bucket) OVER w_all_by_unit_price AS qty_max_2_bucket_change
    FROM sequenced
    WINDOW w_all_by_max_qty_so_far_avg_7 AS (ORDER BY max_qty_so_far_avg_7 ASC, id), w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        ROUND(days_since_moved_at / NULLIF(next2_max_qty_so_far, 0), 3) AS days_since_moved_at_per_next2_max_qty_so_far,
        unit_price + days_since_moved_at AS unit_price_plus_days_since_moved_at
    FROM sequenced_2
    WHERE unit_price <= 387.1
)
-- Final projection
SELECT
    id,
    unit_price,
    days_since_moved_at,
    days_since_moved_at_avg_7,
    unit_price_avg_2_prior,
    unit_price_avg_2_prior_per_qty_max_2,
    qty_max_2_bucket,
    next2_max_qty_so_far,
    max_qty_so_far_avg_7,
    prev_max_qty_so_far,
    max_qty_so_far_avg_7_percent_rank,
    max_qty_so_far_avg_7_cume_dist,
    qty_max_2_bucket_change,
    unit_price_filled,
    days_since_moved_at_per_next2_max_qty_so_far,
    ABS(qty_max_2_bucket - 676) AS qty_max_2_bucket_distance
FROM derived
ORDER BY unit_price_avg_2_prior_per_qty_max_2 DESC, days_since_moved_at_avg_7 DESC
