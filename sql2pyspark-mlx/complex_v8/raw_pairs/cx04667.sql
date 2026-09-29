WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE sku_id <= 13
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
per_group AS (
    -- Group-level totals
    SELECT
        operator,
        PERCENTILE_APPROX(n_events, 0.5) AS median_n_events,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN operator = 'op_chan' THEN session_id ELSE 0 END) AS session_id_operator_op_chan,
        COUNT(DISTINCT operator) AS distinct_operators,
        COUNT(*) FILTER (WHERE operator IN ('op_ann', 'op_bee', 'op_dew')) AS n_operator_in_op_ann_op_bee_op
    FROM session_summary
    WHERE session_qty < 1800
    GROUP BY operator
    HAVING MAX(session_id) > 2
)
-- Final projection
SELECT
    operator,
    median_n_events,
    n_rows,
    session_id_operator_op_chan,
    distinct_operators,
    n_operator_in_op_ann_op_bee_op,
    distinct_operators - median_n_events AS distinct_operators_minus_median_n_events
FROM per_group
ORDER BY n_operator_in_op_ann_op_bee_op NULLS LAST, distinct_operators_minus_median_n_events, operator
LIMIT 10
