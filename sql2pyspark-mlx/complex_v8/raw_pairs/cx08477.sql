WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-08'
      AND warehouse_id >= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(qty_bucket) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_sum_qty_bucket,
        MAX(days_since_moved_at) OVER w_operator_sku_id AS operator_sku_id_max_days_since_moved_at,
        NTH_VALUE(qty_bucket, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_bucket,
        MAX(qty_bucket) OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_bucket_max_7,
        ROUND(AVG(qty_bucket) OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_bucket_avg_7,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER (PARTITION BY operator, sku_id), 0), 4) AS days_since_moved_at_share_of_operator_sku_id,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC, id), 3) AS moved_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC) AS moved_at_dense_rank,
        MIN(qty_bucket) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_min_qty_bucket,
        ROUND(CUME_DIST() OVER w_operator_move_type_by_moved_3, 3) AS moved_at_cume_dist_2,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY qty_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        LAG(moved_at) OVER w_all_by_qty_bucket AS prev_moved_at
    FROM scoped_moves
    WINDOW w_operator_sku_id AS (PARTITION BY operator, sku_id), w_operator_move_type_by_moved_3 AS (PARTITION BY operator, move_type ORDER BY moved_at DESC), w_all_by_qty_bucket AS (ORDER BY qty_bucket, id)
),
summary AS (
    -- Group-level totals
    SELECT
        CASE WHEN second_qty_bucket >= 300 THEN 'large' WHEN second_qty_bucket >= 151 THEN 'medium' ELSE 'small' END AS second_qty_bucket_tier,
        MAX(prev_moved_at) AS last_prev_moved_at,
        ROUND(SUM(moved_at_dense_rank) / NULLIF(SUM(operator_sku_id_sum_qty_bucket), 0), 3) AS moved_at_dense_rank_to_operator_sku_id_sum_qty_b,
        COUNT(*) FILTER (WHERE operator IN ('op_eve', 'op_bee')) AS n_operator_in_op_eve_op_bee
    FROM windowed
    GROUP BY CASE WHEN second_qty_bucket >= 300 THEN 'large' WHEN second_qty_bucket >= 151 THEN 'medium' ELSE 'small' END
    HAVING MAX(qty_bucket_avg_7) > 125.8
)
SELECT
    second_qty_bucket_tier,
    CASE WHEN second_qty_bucket_tier IN ('medium', 'large') THEN 'group_a' WHEN second_qty_bucket_tier = 'small' THEN 'group_b' ELSE 'other' END AS second_qty_bucket_tier_group,
    CASE WHEN second_qty_bucket_tier IN ('large', 'medium') THEN 'group_a' WHEN second_qty_bucket_tier = 'small' THEN 'group_b' ELSE 'other' END AS second_qty_bucket_tier_group_2
FROM summary
