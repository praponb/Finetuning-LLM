WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d,
        CASE WHEN qty >= 300 THEN 'large' WHEN qty >= 111 THEN 'medium' ELSE 'small' END AS qty_tier
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 6 AND 12
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(moved_at) OVER (PARTITION BY sku_id, qty_tier ORDER BY moved_at_plus_1d ASC, id) AS prev_moved_at,
        FIRST_VALUE(moved_at_plus_1d) OVER (PARTITION BY qty_tier ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_plus_1d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, qty_tier ORDER BY moved_at DESC NULLS FIRST), 3) AS moved_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_1d) ORDER BY moved_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, qty_tier ORDER BY moved_at_plus_1d ASC, id), 3) AS moved_at_plus_1d_percent_rank,
        MIN(unit_price) OVER (PARTITION BY move_type, qty_tier) AS move_type_qty_tier_min_unit_price,
        DENSE_RANK() OVER (PARTITION BY qty_tier ORDER BY moved_at DESC) AS moved_at_dense_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_1d) ORDER BY moved_at DESC NULLS FIRST, id) AS unit_price_change,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, qty_tier ORDER BY moved_at DESC NULLS FIRST), 3) AS moved_at_cume_dist,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type, qty_tier)) AS distinct_warehouse_id_in_move_type_qty_tier
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prev_moved_at, 3) OVER (ORDER BY moved_at_plus_1d DESC, id) AS next3_prev_moved_at,
        ROUND(moved_at_cume_dist / NULLIF(SUM(moved_at_cume_dist) OVER w_first_moved_at_plus_1d_month, 0), 4) AS moved_at_cume_dist_share_of_first_moved_at_plus_,
        MIN(unit_price_change) OVER (PARTITION BY MONTH(moved_at)) AS moved_at_month_min_unit_price_change,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY moved_at_dense_rank, sku_id ORDER BY prev_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(moved_at_cume_dist - AVG(moved_at_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', first_moved_at_plus_1d), move_type), 2) AS moved_at_cume_dist_vs_first_moved_at_plus_1d_mon,
        ROUND(minutes_since_prev_moved_at / NULLIF(SUM(minutes_since_prev_moved_at) OVER w_first_moved_at_plus_1d_month, 0), 4) AS minutes_since_prev_moved_at_share_of_first_moved
    FROM with_trends
    WINDOW w_first_moved_at_plus_1d_month AS (PARTITION BY DATE_TRUNC('MONTH', first_moved_at_plus_1d), move_type)
)
SELECT
    id,
    warehouse_id,
    move_type,
    unit_price,
    moved_at_plus_1d,
    qty_tier,
    prev_moved_at,
    first_moved_at_plus_1d,
    moved_at_percent_rank,
    minutes_since_prev_moved_at,
    moved_at_plus_1d_percent_rank,
    move_type_qty_tier_min_unit_price,
    unit_price_change,
    moved_at_cume_dist,
    next3_prev_moved_at,
    moved_at_month_min_unit_price_change,
    moved_at_cume_dist_vs_first_moved_at_plus_1d_mon,
    minutes_since_prev_moved_at_share_of_first_moved
FROM metrics
ORDER BY moved_at_plus_1d DESC
