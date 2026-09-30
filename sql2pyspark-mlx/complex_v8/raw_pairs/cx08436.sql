WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_clean,
        s.moved_at_plus_30m,
        sk.unit_cost AS sku_unit_cost,
        sk.is_active AS sku_is_active
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        qty,
        operator,
        move_type_clean,
        moved_at_plus_30m,
        sku_is_active,
        ROUND(sku_unit_cost * 1.5, 2) AS sku_unit_cost_adj,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM with_details
    WHERE sku_id <= 9
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        operator,
        moved_at_plus_30m,
        sku_is_active,
        operator_ref,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_is_active ORDER BY qty ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior,
        RANK() OVER (PARTITION BY operator ORDER BY qty, id) AS qty_rank
    FROM scored
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        operator,
        moved_at_plus_30m,
        qty_rank,
        LAG(qty_avg_3_prior) OVER (PARTITION BY operator, qty_rank ORDER BY qty DESC, id) AS prev_qty_avg_3_prior,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_is_active, operator), 2) AS qty_vs_sku_is_active_operator_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, qty_rank ORDER BY qty DESC), 3) AS qty_percent_rank,
        ROUND(qty_avg_3_prior / NULLIF(SUM(qty_avg_3_prior) OVER (PARTITION BY sku_is_active, operator), 0), 4) AS qty_avg_3_prior_share_of_sku_is_active_operator
    FROM with_trends
),
totals AS (
    -- Summarise per group
    SELECT
        qty_rank,
        CASE WHEN qty_percent_rank >= 0.8 THEN 'A' WHEN qty_percent_rank >= 0.2 THEN 'B' ELSE 'C' END AS qty_percent_rank_tier,
        MIN(moved_at_plus_30m) AS first_moved_at_plus_30m,
        SUM(qty_rank) AS total_qty_rank,
        COUNT(*) FILTER (WHERE operator IN ('op_bee', 'op_eve')) AS n_operator_in_op_bee_op_eve,
        MIN(qty_percent_rank) AS min_qty_percent_rank,
        MAX(qty) AS max_qty,
        ROUND(SUM(qty_percent_rank) / NULLIF(SUM(prev_qty_avg_3_prior), 0), 3) AS qty_percent_rank_to_prev_qty_avg_3_prior_ratio
    FROM sequenced
    GROUP BY qty_rank, CASE WHEN qty_percent_rank >= 0.8 THEN 'A' WHEN qty_percent_rank >= 0.2 THEN 'B' ELSE 'C' END
    HAVING SUM(qty_percent_rank) > 0.3
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE min_qty_percent_rank < (SELECT MAX(min_qty_percent_rank) FROM totals)
)
-- Final projection
SELECT
    qty_rank,
    qty_percent_rank_tier,
    total_qty_rank,
    min_qty_percent_rank,
    max_qty,
    qty_percent_rank_to_prev_qty_avg_3_prior_ratio,
    ABS(total_qty_rank - 28) AS total_qty_rank_distance
FROM qualified
WHERE qty_percent_rank_tier IN ('C', 'B')
ORDER BY min_qty_percent_rank DESC
