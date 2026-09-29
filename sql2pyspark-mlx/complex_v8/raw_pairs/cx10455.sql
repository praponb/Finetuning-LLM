WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE (moved_at >= TIMESTAMP '2025-02-06 12:00:00' OR qty >= 93)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(operator_len) OVER (PARTITION BY warehouse_id, operator_len) AS warehouse_id_operator_len_min_operator_len,
        DENSE_RANK() OVER w_move_type_by_days_since_move AS days_since_moved_at_dense_rank,
        LAST_VALUE(moved_at_day) OVER (PARTITION BY move_type ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_day,
        ROUND(operator_len - AVG(operator_len) OVER (PARTITION BY warehouse_id, operator_len), 2) AS operator_len_vs_warehouse_id_operator_len_avg,
        ROUND(operator_len / NULLIF(SUM(operator_len) OVER (PARTITION BY warehouse_id, operator_len), 0), 4) AS operator_len_share_of_warehouse_id_operator_len,
        LEAD(moved_at_day) OVER (PARTITION BY move_type ORDER BY days_since_moved_at, id) AS next_moved_at_day
    FROM valid
    WINDOW w_move_type_by_days_since_move AS (PARTITION BY move_type ORDER BY days_since_moved_at)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        days_since_moved_at_dense_rank,
        COUNT(*) FILTER (WHERE operator IS NOT NULL) AS n_operator_is_not_null,
        PERCENTILE_APPROX(operator_len_vs_warehouse_id_operator_len_avg, 0.5) AS median_operator_len_vs_warehouse_id_operator_len,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', moved_at), days_since_moved_at_dense_rank
    HAVING MAX(operator_len_vs_warehouse_id_operator_len_avg) > -9.8
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER w_all_by_n_operator_is_not_nul AS n_operator_is_not_null_decile,
        FIRST_VALUE(n_operator_is_not_null) IGNORE NULLS OVER (ORDER BY n_operator_is_not_null DESC, moved_at_month, days_since_moved_at_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_operator_is_not_null,
        ROUND(AVG(distinct_warehouse_ids) OVER (ORDER BY n_operator_is_not_null DESC, moved_at_month, days_since_moved_at_dense_rank ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_warehouse_ids_avg_2_prior,
        ROUND(AVG(distinct_warehouse_ids) OVER (ORDER BY n_operator_is_not_null DESC, moved_at_month, days_since_moved_at_dense_rank ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS distinct_warehouse_ids_avg_4,
        NTILE(4) OVER (ORDER BY n_operator_is_not_null DESC, moved_at_month, days_since_moved_at_dense_rank) AS n_operator_is_not_null_quartile
    FROM rollup_base
    WINDOW w_all_by_n_operator_is_not_nul AS (ORDER BY n_operator_is_not_null DESC, moved_at_month, days_since_moved_at_dense_rank)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(distinct_warehouse_ids, 0) AS distinct_warehouse_ids_filled,
        ABS(distinct_warehouse_ids - 5) AS distinct_warehouse_ids_distance
    FROM metrics
)
SELECT
    moved_at_month,
    days_since_moved_at_dense_rank,
    median_operator_len_vs_warehouse_id_operator_len,
    distinct_warehouse_ids,
    n_operator_is_not_null_decile,
    first_known_n_operator_is_not_null,
    distinct_warehouse_ids_avg_2_prior,
    distinct_warehouse_ids_avg_4,
    distinct_warehouse_ids_filled,
    distinct_warehouse_ids_distance,
    FLOOR(n_operator_is_not_null / 2) * 2 AS n_operator_is_not_null_bucket
FROM derived
ORDER BY median_operator_len_vs_warehouse_id_operator_len DESC, distinct_warehouse_ids DESC
