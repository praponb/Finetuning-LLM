WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE (qty < 349 OR warehouse_id >= 2)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_week) ORDER BY unit_price_bucket DESC), 3) AS unit_price_bucket_cume_dist,
        ROUND(AVG(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_week) ORDER BY unit_price_bucket DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        NTILE(10) OVER w_warehouse_id_by_unit_price AS unit_price_decile,
        LAST_VALUE(unit_price_bucket) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_week) ORDER BY unit_price_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_bucket,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_operator_move_type, 0), 4) AS unit_price_share_of_operator_move_type,
        MAX(unit_price_bucket) OVER (PARTITION BY move_type ORDER BY moved_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_bucket_so_far,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY unit_price) AS unit_price_dense_rank,
        SUM(unit_price) OVER (PARTITION BY operator ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_sum_2,
        ROUND(unit_price - AVG(unit_price) OVER w_move_type, 2) AS unit_price_vs_move_type_avg,
        LAST_VALUE(unit_price_bucket) OVER (PARTITION BY sku_id ORDER BY unit_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price_bucket
    FROM valid_moves
    WINDOW w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price DESC, id), w_operator_move_type AS (PARTITION BY operator, move_type), w_move_type AS (PARTITION BY move_type)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        moved_at_week,
        unit_price_bucket,
        unit_price_bucket_cume_dist,
        unit_price_avg_6_prior,
        unit_price_decile,
        last_known_unit_price_bucket,
        unit_price_share_of_operator_move_type,
        max_unit_price_bucket_so_far,
        unit_price_sum_2,
        unit_price_vs_move_type_avg,
        last_unit_price_bucket
    FROM with_windows
    WHERE unit_price_dense_rank <= 5
      AND MONTH(moved_at_week) IN (2, 3)
),
stats AS (
    -- Summarise per group
    SELECT
        move_type,
        unit_price_decile,
        warehouse_id,
        COUNT(DISTINCT operator) AS distinct_operators,
        SUM(unit_price) AS total_unit_price,
        ROUND(100 * COUNT(CASE WHEN move_type IN ('TRANSFER', 'ADJUST', 'OUTBOUND') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_transfer_adjust,
        PERCENTILE_APPROX(unit_price_share_of_operator_move_type, 0.5) AS median_unit_price_share_of_operator_move_type,
        ROUND(100 * COUNT(CASE WHEN move_type = 'ADJUST' THEN 1 END) / COUNT(*), 1) AS pct_move_type_adjust
    FROM first_rows
    WHERE unit_price_share_of_operator_move_type <= 0.6
    GROUP BY 1, 2, 3
)
SELECT
    move_type,
    unit_price_decile,
    warehouse_id,
    distinct_operators,
    total_unit_price,
    pct_move_type_in_transfer_adjust,
    median_unit_price_share_of_operator_move_type,
    LENGTH(TRIM(move_type)) AS move_type_len
FROM stats
ORDER BY unit_price_decile NULLS LAST, move_type_len NULLS LAST
