WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CASE WHEN qty >= 349 THEN 'large' WHEN qty >= 198 THEN 'medium' ELSE 'small' END AS qty_tier,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
      AND sku_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(unit_price, 2) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS next2_unit_price,
        ROUND(unit_price_plus_qty - AVG(unit_price_plus_qty) OVER (PARTITION BY move_type), 2) AS unit_price_plus_qty_vs_move_type_avg,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY qty_tier, move_type)) AS distinct_operator_in_qty_tier_move_type,
        ROUND(unit_price_plus_qty - AVG(unit_price_plus_qty) OVER w_qty_tier_move_type, 2) AS unit_price_plus_qty_vs_qty_tier_move_type_avg,
        LEAD(unit_price) OVER (PARTITION BY qty_tier, sku_id ORDER BY moved_at, id) AS next_unit_price,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price) AS unit_price_rank,
        ROUND(AVG(unit_price_plus_qty) OVER (PARTITION BY warehouse_id, qty_tier ORDER BY moved_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_plus_qty_avg_2_prior,
        RANK() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS moved_at_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, qty_tier ORDER BY moved_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        ROUND(AVG(unit_price) OVER w_qty_tier_move_type, 2) AS qty_tier_move_type_avg_unit_price,
        unit_price - LAG(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS unit_price_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, qty_tier ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        FIRST_VALUE(unit_price) OVER (PARTITION BY qty_tier, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price
    FROM base_moves
    WINDOW w_qty_tier_move_type AS (PARTITION BY qty_tier, move_type)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE moved_at_rank <= 3
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    qty_tier,
    unit_price_plus_qty,
    unit_price_plus_qty_vs_move_type_avg,
    distinct_operator_in_qty_tier_move_type,
    unit_price_plus_qty_vs_qty_tier_move_type_avg,
    next_unit_price,
    unit_price_plus_qty_avg_2_prior,
    moved_at_rank,
    qty_tier_move_type_avg_unit_price,
    unit_price_change,
    moved_at_percent_rank,
    first_unit_price,
    ROUND(unit_price * 1.5, 2) AS unit_price_adj
FROM top_per_group
ORDER BY qty_tier DESC, id
LIMIT 5
