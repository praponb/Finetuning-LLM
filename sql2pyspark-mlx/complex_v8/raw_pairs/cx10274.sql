WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE (warehouse_id IS NOT NULL OR moved_at >= TIMESTAMP '2025-01-19 12:00:00')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER w_move_type_by_moved_at AS moved_at_quartile,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY moved_at ASC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY moved_at ASC, id), 3) AS moved_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY moved_at) AS moved_at_dense_rank,
        LAST_VALUE(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY moved_at ASC), 3) AS moved_at_percent_rank_2,
        LEAD(moved_at) OVER w_operator_moved_at_month_by_m AS next_moved_at,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at
    FROM filtered_moves
    WINDOW w_move_type_by_moved_at AS (PARTITION BY move_type ORDER BY moved_at, id), w_operator_moved_at_month_by_m AS (PARTITION BY operator, MONTH(moved_at) ORDER BY moved_at ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE moved_at_dense_rank <= 2
      AND last_moved_at < TIMESTAMP '2025-02-12 13:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(moved_at_percent_rank) OVER (ORDER BY moved_at_quartile DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS moved_at_percent_rank_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(moved_at), moved_at_quartile ORDER BY moved_at) AS moved_at_dense_rank_2,
        ROUND(moved_at_quartile / NULLIF(SUM(moved_at_quartile) OVER (PARTITION BY sku_id), 0), 4) AS moved_at_quartile_share_of_sku_id,
        LEAD(moved_at_dense_rank) OVER (ORDER BY moved_at_dense_rank, id) AS next_moved_at_dense_rank,
        COUNT(*) OVER w_next_moved_at_to_date_operat AS rows_in_next_moved_at_to_date_operator,
        SUM(moved_at_percent_rank) OVER (PARTITION BY TO_DATE(next_moved_at), operator) AS next_moved_at_to_date_operator_sum_moved_at_perc,
        FIRST_VALUE(moved_at_percent_rank) OVER (ORDER BY moved_at_quartile DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_percent_rank,
        MIN(moved_at_dense_rank) OVER (PARTITION BY sku_id) AS sku_id_min_moved_at_dense_rank
    FROM leaders
    WINDOW w_next_moved_at_to_date_operat AS (PARTITION BY TO_DATE(next_moved_at), operator)
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    operator,
    move_type_operator_label,
    minutes_since_prev_moved_at,
    moved_at_percent_rank,
    moved_at_dense_rank,
    moved_at_percent_rank_2,
    second_moved_at,
    moved_at_percent_rank_avg_2_prior,
    moved_at_dense_rank_2,
    next_moved_at_dense_rank,
    rows_in_next_moved_at_to_date_operator,
    next_moved_at_to_date_operator_sum_moved_at_perc,
    first_moved_at_percent_rank,
    sku_id_min_moved_at_dense_rank,
    HOUR(moved_at) AS moved_at_hour,
    ABS(next_moved_at_dense_rank - 6) AS next_moved_at_dense_rank_distance
FROM metrics
WHERE sku_id_min_moved_at_dense_rank <= 70
