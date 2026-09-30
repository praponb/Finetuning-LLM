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
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id), 3) AS moved_at_percent_rank,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER w_sku_id, 0), 4) AS days_since_moved_at_share_of_sku_id,
        NTH_VALUE(days_since_moved_at, 2) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_2,
        FIRST_VALUE(unit_price) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price
    FROM prepared
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        moved_at,
        operator,
        days_since_moved_at,
        moved_at_percent_rank,
        second_unit_price,
        sku_id_avg_qty,
        days_since_moved_at_share_of_sku_id,
        second_days_since_moved_at,
        second_moved_at,
        second_unit_price_2,
        first_unit_price,
        MIN(unit_price) OVER (PARTITION BY operator) AS operator_min_unit_price,
        MIN(days_since_moved_at) OVER w_operator AS operator_min_days_since_moved_at,
        MAX(second_days_since_moved_at) OVER (PARTITION BY operator) AS operator_max_second_days_since_moved_at,
        LAST_VALUE(unit_price) OVER (PARTITION BY operator ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator)) AS distinct_move_type_in_operator,
        LAST_VALUE(sku_id_avg_qty) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY first_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_id_avg_qty
    FROM with_trends
    WINDOW w_operator AS (PARTITION BY operator)
),
grouped AS (
    -- Group-level totals
    SELECT
        CASE WHEN sku_id_avg_qty >= 350.3 THEN 'large' WHEN sku_id_avg_qty >= 150.7 THEN 'medium' ELSE 'small' END AS sku_id_avg_qty_tier,
        TO_DATE(moved_at) AS moved_at_day,
        distinct_move_type_in_operator,
        ROUND(AVG(first_unit_price), 2) AS avg_first_unit_price,
        MIN(sku_id_avg_qty) AS min_sku_id_avg_qty,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        ROUND(SUM(moved_at_percent_rank), 2) AS total_moved_at_percent_rank,
        SUM(CASE WHEN operator NOT IN ('op_ann', 'op_bee') THEN 1 ELSE 0 END) AS n_operator_not_in_op_ann_op_be,
        SUM(operator_min_unit_price) AS total_operator_min_unit_price
    FROM with_windows
    GROUP BY CASE WHEN sku_id_avg_qty >= 350.3 THEN 'large' WHEN sku_id_avg_qty >= 150.7 THEN 'medium' ELSE 'small' END, TO_DATE(moved_at), distinct_move_type_in_operator
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_moved_at_day_month AS rows_in_moved_at_day_month,
        SIZE(COLLECT_SET(sku_id_avg_qty_tier) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_day))) AS distinct_sku_id_avg_qty_tier_in_moved_at_day_mon,
        LAST_VALUE(min_sku_id_avg_qty) IGNORE NULLS OVER (PARTITION BY sku_id_avg_qty_tier ORDER BY n_operator_not_in_op_ann_op_be, sku_id_avg_qty_tier, moved_at_day, distinct_move_type_in_operator ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_min_sku_id_avg_qty
    FROM grouped
    WINDOW w_moved_at_day_month AS (PARTITION BY DATE_TRUNC('MONTH', moved_at_day))
)
-- Final projection
SELECT
    sku_id_avg_qty_tier,
    moved_at_day,
    distinct_move_type_in_operator,
    avg_first_unit_price,
    min_sku_id_avg_qty,
    operator_list,
    n_operator_not_in_op_ann_op_be,
    distinct_sku_id_avg_qty_tier_in_moved_at_day_mon,
    last_known_min_sku_id_avg_qty,
    ABS(min_sku_id_avg_qty - 325.4) AS min_sku_id_avg_qty_distance,
    ROUND(avg_first_unit_price * 0.25, 2) AS avg_first_unit_price_adj
FROM with_trends_2
ORDER BY distinct_move_type_in_operator DESC, avg_first_unit_price
