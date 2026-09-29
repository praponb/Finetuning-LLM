WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
    WHERE unit_price < 398.5
      AND sku_id IS NOT NULL
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.operator_or_unknown,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM cleaned s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND HOUR(st.moved_at) BETWEEN 10 AND 15
),
rollup_base AS (
    -- Summarise per group
    SELECT
        operator,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN move_operator NOT IN ('op_eve', 'op_chan') THEN 1 END) / COUNT(*), 1) AS pct_move_operator_not_in_op_eve
    FROM exploded_detail
    GROUP BY 1
    HAVING AVG(qty) > 123
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY pct_move_operator_not_in_op_eve DESC), 3) AS pct_move_operator_not_in_op_eve_percent_rank,
        SUM(row_count) OVER (PARTITION BY row_count) AS row_count_sum_row_count,
        NTILE(3) OVER (ORDER BY pct_move_operator_not_in_op_eve DESC, operator) AS pct_move_operator_not_in_op_eve_tercile
    FROM rollup_base
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        operator,
        row_count,
        pct_move_operator_not_in_op_eve_percent_rank,
        FIRST_VALUE(row_count_sum_row_count) OVER (PARTITION BY pct_move_operator_not_in_op_eve_tercile ORDER BY row_count_sum_row_count, operator ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_row_count_sum_row_count,
        ROUND(row_count_sum_row_count / NULLIF(SUM(row_count_sum_row_count) OVER (PARTITION BY row_count, pct_move_operator_not_in_op_eve_tercile), 0), 4) AS row_count_sum_row_count_share_of_row_count_pct_m,
        ROUND(PERCENT_RANK() OVER w_pct_move_operator_not_in_op_2, 3) AS row_count_sum_row_count_percent_rank,
        COUNT(*) OVER (PARTITION BY row_count, pct_move_operator_not_in_op_eve_tercile) AS rows_in_row_count_pct_move_operator_not_in_op_ev,
        NTILE(3) OVER w_pct_move_operator_not_in_op_3 AS row_count_sum_row_count_tercile,
        RANK() OVER w_pct_move_operator_not_in_op_2 AS row_count_sum_row_count_rank
    FROM windowed
    WINDOW w_pct_move_operator_not_in_op_2 AS (PARTITION BY pct_move_operator_not_in_op_eve_tercile ORDER BY row_count_sum_row_count), w_pct_move_operator_not_in_op_3 AS (PARTITION BY pct_move_operator_not_in_op_eve_tercile ORDER BY row_count_sum_row_count, operator)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        operator,
        row_count,
        pct_move_operator_not_in_op_eve_percent_rank,
        row_count_sum_row_count_share_of_row_count_pct_m,
        row_count_sum_row_count_percent_rank,
        rows_in_row_count_pct_move_operator_not_in_op_ev,
        row_count_sum_row_count_tercile,
        row_count_sum_row_count_rank,
        ROW_NUMBER() OVER (PARTITION BY row_count_sum_row_count_tercile ORDER BY row_count_sum_row_count_percent_rank, operator) AS row_count_sum_row_count_percent_rank_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count_sum_row_count_rank, operator ORDER BY row_count_sum_row_count_percent_rank DESC), 3) AS row_count_sum_row_count_percent_rank_percent_ran
    FROM metrics
)
SELECT
    operator,
    row_count,
    row_count_sum_row_count_share_of_row_count_pct_m,
    row_count_sum_row_count_percent_rank,
    rows_in_row_count_pct_move_operator_not_in_op_ev,
    row_count_sum_row_count_tercile,
    row_count_sum_row_count_rank,
    row_count_sum_row_count_percent_rank_seq,
    row_count_sum_row_count_percent_rank_percent_ran,
    LENGTH(TRIM(operator)) AS operator_len,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM ranked
WHERE row_count_sum_row_count_percent_rank_seq > 2
ORDER BY operator_or_unknown NULLS LAST, row_count_sum_row_count_rank DESC
