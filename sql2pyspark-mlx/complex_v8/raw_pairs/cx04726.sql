WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_chan')
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_no
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY YEAR(session_end), session_no)) AS distinct_operator_in_session_end_year_session_no,
        COUNT(*) OVER (PARTITION BY YEAR(session_end), session_no) AS rows_in_session_end_year_session_no,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY session_start DESC) AS session_start_dense_rank
    FROM session_summary
)
-- Final projection
SELECT
    operator,
    session_no,
    events,
    distinct_operator_in_session_end_year_session_no,
    rows_in_session_end_year_session_no,
    session_start_dense_rank
FROM with_windows
ORDER BY distinct_operator_in_session_end_year_session_no
