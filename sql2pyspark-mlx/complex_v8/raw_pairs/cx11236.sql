WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE operator = 'op_bee'
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.days_since_moved_at,
        sk.is_active AS sku_is_active
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id AND SIZE(sk.labels) >= 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(days_since_moved_at) OVER (ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_moved_at,
        LAG(days_since_moved_at) OVER (PARTITION BY move_type, sku_is_active ORDER BY days_since_moved_at DESC, id) AS prev_days_since_moved_at
    FROM with_parent
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        operator,
        days_since_moved_at,
        ROUND(AVG(prev_days_since_moved_at) OVER (PARTITION BY sku_id ORDER BY days_since_moved_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prev_days_since_moved_at_avg_3_prior,
        LAG(days_since_moved_at, 1, 0) OVER w_move_type_by_prev_days_since AS prev_days_since_moved_at_2,
        DENSE_RANK() OVER w_move_type_by_prev_days_since_2 AS prev_days_since_moved_at_dense_rank
    FROM ranked
    WINDOW w_move_type_by_prev_days_since AS (PARTITION BY move_type ORDER BY prev_days_since_moved_at DESC NULLS FIRST, id), w_move_type_by_prev_days_since_2 AS (PARTITION BY move_type ORDER BY prev_days_since_moved_at, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        prev_days_since_moved_at_dense_rank,
        MIN(prev_days_since_moved_at_dense_rank) AS min_prev_days_since_moved_at_dense_rank,
        MAX(days_since_moved_at) AS max_days_since_moved_at,
        MIN(prev_days_since_moved_at_2) AS min_prev_days_since_moved_at_2,
        COUNT(*) AS row_count
    FROM with_windows
    GROUP BY operator, prev_days_since_moved_at_dense_rank
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        CASE WHEN operator IN ('op_chan', 'op_dew') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM per_group
)
SELECT operator, prev_days_since_moved_at_dense_rank, metric, metric_value
FROM (
    SELECT operator, prev_days_since_moved_at_dense_rank, row_count, max_days_since_moved_at
    FROM labeled
) src
UNPIVOT (
    metric_value FOR metric IN (row_count, max_days_since_moved_at)
) unp
