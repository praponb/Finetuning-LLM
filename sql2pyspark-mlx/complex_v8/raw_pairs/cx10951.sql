WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id >= 5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC), 3) AS moved_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY moved_at DESC NULLS FIRST, id), 3) AS moved_at_cume_dist,
        DENSE_RANK() OVER w_move_type_warehouse_id_by_mo AS moved_at_dense_rank,
        NTILE(4) OVER w_warehouse_id_move_type_by_mo AS moved_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS seq_no,
        LAST_VALUE(moved_at) OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(PERCENT_RANK() OVER w_all_by_moved_at, 3) AS moved_at_percent_rank_2,
        ROUND(CUME_DIST() OVER w_all_by_moved_at_ties, 3) AS moved_at_cume_dist_2,
        LEAD(moved_at, 3) OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC, id) AS next3_moved_at,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY moved_at DESC NULLS FIRST) AS moved_at_rank
    FROM filtered
    WINDOW w_move_type_warehouse_id_by_mo AS (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC), w_warehouse_id_move_type_by_mo AS (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id), w_all_by_moved_at AS (ORDER BY moved_at, id), w_all_by_moved_at_ties AS (ORDER BY moved_at)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(moved_at_rank) OVER (PARTITION BY operator ORDER BY next3_moved_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS moved_at_rank_max_3_prior,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        ROUND((UNIX_TIMESTAMP(last_moved_at) - UNIX_TIMESTAMP(LAG(last_moved_at) OVER (PARTITION BY moved_at_rank ORDER BY moved_at_rank DESC, id))) / 60, 1) AS minutes_since_prev_last_moved_at,
        ROUND(moved_at_dense_rank - AVG(moved_at_dense_rank) OVER w_sku_id, 2) AS moved_at_dense_rank_vs_sku_id_avg,
        FIRST_VALUE(moved_at_dense_rank) OVER (PARTITION BY TO_DATE(moved_at) ORDER BY last_moved_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_dense_rank,
        FIRST_VALUE(moved_at_percent_rank) IGNORE NULLS OVER (PARTITION BY moved_at_rank ORDER BY moved_at_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_percent_rank
    FROM metrics
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_operator AS (PARTITION BY operator)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    moved_at_percent_rank,
    moved_at_cume_dist,
    moved_at_dense_rank,
    moved_at_quartile,
    seq_no,
    moved_at_cume_dist_2,
    next3_moved_at,
    moved_at_rank,
    moved_at_rank_max_3_prior,
    rows_in_operator,
    moved_at_dense_rank_vs_sku_id_avg,
    first_moved_at_dense_rank,
    first_known_moved_at_percent_rank
FROM with_windows
ORDER BY moved_at_rank_max_3_prior DESC, seq_no, id
LIMIT 5
