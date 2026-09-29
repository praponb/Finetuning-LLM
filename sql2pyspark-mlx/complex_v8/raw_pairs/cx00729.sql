WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 7 AND 11
),
totals AS (
    -- Group-level totals
    SELECT
        move_type,
        MIN(unit_price) AS min_unit_price,
        ROUND(100 * COUNT(CASE WHEN move_type NOT IN ('INBOUND', 'ADJUST') THEN 1 END) / COUNT(*), 1) AS pct_move_type_not_in_inbound_adj
    FROM filtered_moves
    WHERE operator = 'op_eve'
    GROUP BY move_type
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE pct_move_type_not_in_inbound_adj < (SELECT MAX(pct_move_type_not_in_inbound_adj) FROM totals)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        move_type,
        FIRST_VALUE(min_unit_price) OVER (ORDER BY pct_move_type_not_in_inbound_adj ASC, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_unit_price,
        ROUND(min_unit_price / NULLIF(SUM(min_unit_price) OVER (PARTITION BY move_type), 0), 4) AS min_unit_price_share_of_move_type,
        LAG(min_unit_price, 2, 0) OVER (ORDER BY pct_move_type_not_in_inbound_adj ASC, move_type) AS prev2_min_unit_price
    FROM filtered
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY prev2_min_unit_price DESC NULLS FIRST), 3) AS prev2_min_unit_price_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY min_unit_price_share_of_move_type DESC, move_type), 3) AS min_unit_price_share_of_move_type_percent_rank,
        ROUND(prev2_min_unit_price / NULLIF(SUM(prev2_min_unit_price) OVER (PARTITION BY move_type), 0), 4) AS prev2_min_unit_price_share_of_move_type,
        ROUND(first_min_unit_price - AVG(first_min_unit_price) OVER (PARTITION BY move_type), 2) AS first_min_unit_price_vs_move_type_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_prev2_min_unit_price, 3) AS prev2_min_unit_price_percent_rank,
        ROUND(AVG(first_min_unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_first_min_unit_price
    FROM metrics
    WINDOW w_all_by_prev2_min_unit_price AS (ORDER BY prev2_min_unit_price DESC NULLS FIRST)
)
-- Final projection
SELECT
    move_type,
    first_min_unit_price,
    prev2_min_unit_price_cume_dist,
    prev2_min_unit_price_share_of_move_type,
    first_min_unit_price_vs_move_type_avg,
    prev2_min_unit_price_percent_rank,
    move_type_avg_first_min_unit_price,
    prev2_min_unit_price - first_min_unit_price AS prev2_min_unit_price_minus_first_min_unit_price,
    UPPER(TRIM(move_type)) AS move_type_clean
FROM sequenced
WHERE prev2_min_unit_price_percent_rank > 0.2
ORDER BY first_min_unit_price_vs_move_type_avg, prev2_min_unit_price_minus_first_min_unit_price DESC NULLS FIRST, move_type
LIMIT 5
