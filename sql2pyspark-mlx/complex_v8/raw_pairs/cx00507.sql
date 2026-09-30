WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 07:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at DESC), 3) AS moved_at_percent_rank,
        MAX(unit_price) OVER (PARTITION BY warehouse_id, move_type) AS warehouse_id_move_type_max_unit_price,
        ROUND(AVG(unit_price) OVER w_warehouse_id_move_type, 2) AS warehouse_id_move_type_avg_unit_price,
        unit_price - LAG(unit_price) OVER (PARTITION BY move_type_or_unknown, move_type ORDER BY moved_at NULLS LAST, id) AS unit_price_change,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_id_move_type, 2) AS unit_price_vs_warehouse_id_move_type_avg,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type_or_unknown, move_type ORDER BY moved_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_2_prior,
        COUNT(*) OVER w_warehouse_id_move_type AS rows_in_warehouse_id_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id) AS rn
    FROM filtered_moves
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at_percent_rank,
        warehouse_id_move_type_avg_unit_price,
        unit_price_change,
        unit_price_vs_warehouse_id_move_type_avg,
        unit_price_avg_2_prior,
        rows_in_warehouse_id_move_type,
        unit_price_avg_1_prior,
        rn,
        NTILE(4) OVER w_moved_at_month_move_type_by AS unit_price_vs_warehouse_id_move_type_avg_quartil,
        ROUND(AVG(unit_price_avg_1_prior) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior_avg_2_prior,
        NTILE(5) OVER w_all_by_unit_price AS unit_price_quintile,
        ROUND(AVG(unit_price_avg_1_prior) OVER (ORDER BY unit_price_avg_1_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_1_prior_avg_4,
        SIZE(COLLECT_SET(move_type_or_unknown) OVER w_sku_id) AS distinct_move_type_or_unknown_in_sku_id
    FROM ranked
    WINDOW w_moved_at_month_move_type_by AS (PARTITION BY MONTH(moved_at), move_type ORDER BY unit_price_vs_warehouse_id_move_type_avg DESC, id), w_all_by_unit_price AS (ORDER BY unit_price, id), w_sku_id AS (PARTITION BY sku_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        rn,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE move_type IN ('ADJUST', 'TRANSFER')) AS n_move_type_in_adjust_transfer,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        ROUND(AVG(rn), 2) AS avg_rn,
        PERCENTILE_APPROX(distinct_move_type_or_unknown_in_sku_id, 0.5) AS median_distinct_move_type_or_unknown_in_sku_id
    FROM windowed
    GROUP BY 1
    HAVING AVG(unit_price_avg_1_prior_avg_4) > 150.7
)
-- Final projection
SELECT
    rn,
    row_count,
    n_move_type_in_adjust_transfer,
    median_distinct_move_type_or_unknown_in_sku_id,
    COALESCE(rn, 0) AS rn_filled,
    row_count - avg_rn AS row_count_minus_avg_rn
FROM grouped
ORDER BY rn_filled
