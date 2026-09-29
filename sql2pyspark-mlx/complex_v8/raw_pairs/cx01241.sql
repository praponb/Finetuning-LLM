WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'TRANSFER', 'ADJUST')
      AND moved_at >= TIMESTAMP '2025-01-31 08:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM prepared_moves
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_operator_move_type_by_moved AS moved_at_decile,
        COUNT(*) OVER w_operator_move_type AS rows_in_operator_move_type,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_operator_move_type_by_moved)) / 60, 1) AS minutes_since_prev_moved_at
    FROM tagged
    WINDOW w_operator_move_type_by_moved AS (PARTITION BY operator, move_type ORDER BY moved_at, id), w_operator_move_type AS (PARTITION BY operator, move_type)
),
summary AS (
    -- Summarise per group
    SELECT
        operator,
        move_type,
        ROUND(SUM(minutes_since_prev_moved_at) / NULLIF(SUM(rows_in_operator_move_type), 0), 3) AS minutes_since_prev_moved_at_to_rows_in_operator_,
        SUM(moved_at_decile) AS total_moved_at_decile,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM ranked
    GROUP BY operator, move_type
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        operator,
        move_type,
        n_rows,
        operator_list,
        MAX(total_moved_at_decile) OVER (PARTITION BY operator, n_rows) AS operator_n_rows_max_total_moved_at_decile,
        MAX(n_rows) OVER w_operator_n_rows AS operator_n_rows_max_n_rows,
        total_moved_at_decile - LAG(total_moved_at_decile) OVER (PARTITION BY move_type ORDER BY n_rows, operator, move_type) AS total_moved_at_decile_change,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator, n_rows)) AS distinct_move_type_in_operator_n_rows,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY n_rows, operator, move_type) AS rn,
        LAG(total_moved_at_decile) OVER (PARTITION BY move_type ORDER BY n_rows, operator, move_type) AS prev_total_moved_at_decile
    FROM summary
    WINDOW w_operator_n_rows AS (PARTITION BY operator, n_rows)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE rn < (SELECT MAX(rn) FROM with_windows)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY rn, distinct_move_type_in_operator_n_rows ORDER BY total_moved_at_decile_change DESC, operator, move_type) AS seq_no,
        rn - LAG(rn) OVER (PARTITION BY rn, distinct_move_type_in_operator_n_rows ORDER BY total_moved_at_decile_change DESC, operator, move_type) AS rn_change,
        SUM(rn) OVER (PARTITION BY n_rows) AS n_rows_sum_rn,
        SIZE(COLLECT_SET(move_type) OVER w_n_rows) AS distinct_move_type_in_n_rows
    FROM qualified
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
-- Final projection
SELECT
    operator,
    n_rows,
    operator_list,
    operator_n_rows_max_total_moved_at_decile,
    operator_n_rows_max_n_rows,
    total_moved_at_decile_change,
    distinct_move_type_in_operator_n_rows,
    rn,
    seq_no,
    rn_change,
    n_rows_sum_rn,
    distinct_move_type_in_n_rows,
    LEAST(rn_change, seq_no) AS least_rn_change_seq_no,
    CASE WHEN move_type IN ('INBOUND', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM with_trends
ORDER BY operator_n_rows_max_n_rows DESC, seq_no DESC NULLS FIRST
