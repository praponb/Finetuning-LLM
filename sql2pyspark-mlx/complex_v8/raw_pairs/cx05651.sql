WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id <= 5
      AND sku_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_2,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at ASC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM filtered
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_moved_at) AS second_moved_at_month,
        sku_id,
        MIN(minutes_since_prev_moved_at) AS min_minutes_since_prev_moved_at,
        COUNT(*) AS cnt,
        ROUND(STDDEV(minutes_since_prev_moved_at), 2) AS stddev_minutes_since_prev_moved_at,
        MIN(first_known_moved_at) AS first_first_known_moved_at
    FROM metrics
    WHERE moved_at >= TIMESTAMP '2025-02-12 08:00:00'
    GROUP BY DATE_TRUNC('MONTH', second_moved_at), sku_id
    HAVING MAX(minutes_since_prev_moved_at) > 1500.0
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.second_moved_at_month,
        s.sku_id,
        s.min_minutes_since_prev_moved_at,
        s.stddev_minutes_since_prev_moved_at,
        s.first_first_known_moved_at,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM per_group s
    JOIN skus sk ON sk.id = s.sku_id AND LENGTH(TRIM(sk.sku_code)) >= 7
    WHERE sk.weight_kg > 6.4
)
-- Final projection
SELECT
    second_moved_at_month,
    sku_id,
    stddev_minutes_since_prev_moved_at,
    first_first_known_moved_at,
    sku_weight_kg,
    sku_labels,
    sku_is_active,
    DATE_TRUNC('WEEK', second_moved_at_month) AS second_moved_at_month_week,
    COALESCE(sku_weight_kg, 0.0) AS sku_weight_kg_filled
FROM combined_details_sku
ORDER BY sku_id DESC
