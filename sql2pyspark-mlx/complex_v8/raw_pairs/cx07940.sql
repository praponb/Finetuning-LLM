WITH prepared_moves AS (
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
        UPPER(TRIM(move_type)) AS move_type_clean,
        LOWER(TRIM(move_type)) AS move_type_clean_2,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
    WHERE operator NOT IN ('op_chan', 'op_eve')
),
rollup_base AS (
    -- Summarise per group
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        operator,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS n_rows
    FROM prepared_moves
    WHERE unit_price >= 119.5
    GROUP BY TO_DATE(moved_at), operator
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE n_rows < (SELECT MAX(n_rows) FROM rollup_base)
),
qualified_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM qualified
    WHERE n_rows < (SELECT MAX(n_rows) FROM qualified)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(distinct_move_types) OVER (PARTITION BY operator, MONTH(last_moved_at)) AS operator_last_moved_at_month_sum_distinct_move_t,
        LAG(distinct_move_types, 2, 0) OVER (ORDER BY distinct_move_types, moved_at_day, operator) AS prev2_distinct_move_types,
        ROUND(AVG(n_rows) OVER (ORDER BY distinct_move_types, moved_at_day, operator ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS n_rows_avg_3_prior
    FROM qualified_2
)
SELECT
    moved_at_day,
    operator,
    distinct_move_types,
    move_type_list,
    last_moved_at,
    n_rows,
    prev2_distinct_move_types,
    n_rows_avg_3_prior,
    DATEDIFF(DATE '2025-06-30', moved_at_day) AS days_since_moved_at_day,
    ABS(n_rows - 7) AS n_rows_distance
FROM windowed
ORDER BY moved_at_day, n_rows_avg_3_prior
