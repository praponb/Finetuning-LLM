WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type NOT IN ('ADJUST', 'TRANSFER')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, sku_id), 2) AS qty_vs_operator_sku_id_avg,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, sku_id)) AS distinct_warehouse_id_in_operator_sku_id,
        DENSE_RANK() OVER (ORDER BY unit_price DESC, id) AS unit_price_dense_rank,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY qty DESC) AS qty_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator)) AS distinct_warehouse_id_in_operator,
        ROUND(AVG(unit_price) OVER (ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2,
        NTILE(4) OVER (ORDER BY moved_at DESC NULLS FIRST, id) AS moved_at_quartile,
        SUM(qty) OVER (PARTITION BY operator) AS operator_sum_qty,
        MIN(qty) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        MAX(qty) OVER w_operator AS operator_max_qty,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_sum_3,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7
    FROM base_moves
    WINDOW w_operator AS (PARTITION BY operator)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        qty_vs_operator_sku_id_avg,
        unit_price_dense_rank,
        qty_rank,
        unit_price_avg_2,
        moved_at_quartile,
        min_qty_so_far,
        qty_sum_3,
        unit_price_avg_7,
        ABS(operator_max_qty - 3250) AS operator_max_qty_distance,
        FLOOR(min_qty_so_far / 1000) * 1000 AS min_qty_so_far_bucket
    FROM ranked
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_qty_rank_moved_at_quartile_b, 3) AS qty_vs_operator_sku_id_avg_percent_rank,
        unit_price_avg_2 - LAG(unit_price_avg_2) OVER (ORDER BY unit_price_avg_2, id) AS unit_price_avg_2_change,
        LEAD(unit_price_dense_rank) OVER w_all_by_unit_price_avg_2 AS next_unit_price_dense_rank,
        ROUND(qty_vs_operator_sku_id_avg / NULLIF(SUM(qty_vs_operator_sku_id_avg) OVER (PARTITION BY sku_id), 0), 4) AS qty_vs_operator_sku_id_avg_share_of_sku_id,
        ROUND(AVG(moved_at_quartile) OVER (PARTITION BY qty_rank, moved_at_quartile ORDER BY qty_vs_operator_sku_id_avg, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS moved_at_quartile_avg_4,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_unit_price
    FROM labeled
    WINDOW w_qty_rank_moved_at_quartile_b AS (PARTITION BY qty_rank, moved_at_quartile ORDER BY qty_vs_operator_sku_id_avg), w_all_by_unit_price_avg_2 AS (ORDER BY unit_price_avg_2, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        next_unit_price_dense_rank,
        sku_id,
        PERCENTILE_APPROX(moved_at_quartile, 0.5) AS median_moved_at_quartile,
        COUNT(*) AS n_rows,
        MIN(qty_rank) AS min_qty_rank,
        PERCENTILE_APPROX(moved_at_quartile_avg_4, 0.5) AS median_moved_at_quartile_avg_4,
        ROUND(STDDEV(moved_at_quartile), 2) AS stddev_moved_at_quartile
    FROM ranked_2
    GROUP BY next_unit_price_dense_rank, sku_id
    HAVING COUNT(*) >= 3
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        next_unit_price_dense_rank,
        sku_id,
        median_moved_at_quartile,
        n_rows,
        min_qty_rank,
        median_moved_at_quartile_avg_4,
        stddev_moved_at_quartile,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_qty_rank DESC), 3) AS min_qty_rank_percent_rank,
        RANK() OVER (ORDER BY median_moved_at_quartile DESC, next_unit_price_dense_rank, sku_id) AS median_moved_at_quartile_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_n_rows_min_qty_rank) AS distinct_sku_id_in_n_rows_min_qty_rank,
        NTILE(5) OVER (ORDER BY median_moved_at_quartile DESC, next_unit_price_dense_rank, sku_id) AS median_moved_at_quartile_quintile
    FROM per_group
    WINDOW w_n_rows_min_qty_rank AS (PARTITION BY n_rows, min_qty_rank)
)
SELECT
    next_unit_price_dense_rank,
    sku_id,
    median_moved_at_quartile,
    n_rows,
    median_moved_at_quartile_avg_4,
    stddev_moved_at_quartile,
    min_qty_rank_percent_rank,
    median_moved_at_quartile_rank,
    median_moved_at_quartile_quintile
FROM sequenced
WHERE median_moved_at_quartile_avg_4 BETWEEN 2.8 AND 6.4
ORDER BY next_unit_price_dense_rank DESC NULLS FIRST, sku_id
LIMIT 5
