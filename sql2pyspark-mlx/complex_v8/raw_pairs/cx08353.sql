WITH cleaned_moves AS (
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
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND sku_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        MAX(days_since_moved_at) OVER w_sku_id AS sku_id_max_days_since_moved_at,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        LAG(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_qty,
        SUM(days_since_moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_moved_at_sum_6_prior,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_days_since_moved_at,
        SUM(days_since_moved_at) OVER w_sku_id AS sku_id_sum_days_since_moved_at,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_moved_at_avg_7,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id)) AS distinct_move_type_in_sku_id,
        LAG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id) AS prev_unit_price,
        MAX(qty) OVER (PARTITION BY sku_id) AS sku_id_max_qty,
        MIN(unit_price) OVER w_sku_id AS sku_id_min_unit_price,
        LEAD(moved_at) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS next_moved_at
    FROM cleaned_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', next_moved_at) ORDER BY days_since_moved_at_sum_6_prior, id) AS days_since_moved_at_sum_6_prior_quartile,
        ROUND(AVG(days_since_moved_at_avg_7) OVER (PARTITION BY move_type), 2) AS move_type_avg_days_since_moved_at_avg_7,
        LAST_VALUE(prev_qty) OVER (PARTITION BY DATE_TRUNC('MONTH', next_moved_at) ORDER BY days_since_moved_at_sum_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_qty,
        NTILE(5) OVER (PARTITION BY distinct_move_type_in_sku_id, move_type ORDER BY distinct_move_type_in_sku_id, id) AS distinct_move_type_in_sku_id_quintile
    FROM metrics
)
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    sku_id_max_days_since_moved_at,
    sku_id_avg_qty,
    prev_qty,
    days_since_moved_at_sum_6_prior,
    sku_id_avg_days_since_moved_at,
    sku_id_sum_days_since_moved_at,
    sku_id_max_qty,
    sku_id_min_unit_price,
    next_moved_at,
    days_since_moved_at_sum_6_prior_quartile,
    move_type_avg_days_since_moved_at_avg_7,
    last_prev_qty,
    distinct_move_type_in_sku_id_quintile,
    DATE_TRUNC('QUARTER', next_moved_at) AS next_moved_at_quarter
FROM ranked
WHERE days_since_moved_at_sum_6_prior_quartile > 4
ORDER BY sku_id_avg_days_since_moved_at DESC
