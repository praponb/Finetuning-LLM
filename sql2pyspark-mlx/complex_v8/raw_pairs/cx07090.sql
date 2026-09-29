WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 13
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
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
)
-- Final projection
SELECT
    operator,
    session_id,
    session_start,
    session_end,
    n_events,
    session_qty,
    duration_min,
    MONTH(session_end) AS session_end_month,
    GREATEST(session_qty, n_events) AS greatest_session_qty_n_events
FROM sessions
ORDER BY operator
