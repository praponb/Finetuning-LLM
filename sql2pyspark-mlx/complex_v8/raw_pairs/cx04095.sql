WITH valid AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE sku_id >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id)) AS distinct_operator_in_sku_id,
        MAX(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior,
        RANK() OVER (PARTITION BY operator, move_type ORDER BY unit_price DESC) AS unit_price_rank,
        FIRST_VALUE(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROUND(PERCENT_RANK() OVER w_operator_move_type_by_unit_p, 3) AS unit_price_percent_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg,
        unit_price - LAG(unit_price) OVER (ORDER BY moved_at DESC, id) AS unit_price_change,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_3,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type), 0), 4) AS unit_price_share_of_move_type
    FROM valid
    WINDOW w_operator_move_type_by_unit_p AS (PARTITION BY operator, move_type ORDER BY unit_price DESC)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        distinct_operator_in_sku_id,
        ROUND(SUM(first_unit_price) / NULLIF(SUM(unit_price_max_2_prior), 0), 3) AS first_unit_price_to_unit_price_max_2_prior_ratio,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        ROUND(SUM(unit_price_percent_rank), 2) AS total_unit_price_percent_rank
    FROM windowed
    GROUP BY 1, 2
    HAVING SUM(unit_price_max_2_prior) > 150.9
)
-- Final projection
SELECT
    move_type,
    distinct_operator_in_sku_id,
    first_unit_price_to_unit_price_max_2_prior_ratio,
    distinct_move_types,
    total_unit_price_percent_rank,
    COALESCE(distinct_move_types, 0) AS distinct_move_types_filled
FROM rollup_base
WHERE distinct_operator_in_sku_id < 9
