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
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
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
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE session_id > (SELECT AVG(session_id) FROM per_session)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        session_id,
        ROUND(SUM(duration_min) / NULLIF(SUM(n_events), 0), 3) AS duration_min_to_n_events_ratio,
        ROUND(100 * COUNT(CASE WHEN operator NOT IN ('op_eve', 'op_bee') THEN 1 END) / COUNT(*), 1) AS pct_operator_not_in_op_eve_op_be,
        COUNT(DISTINCT operator) AS distinct_operators,
        ROUND(AVG(session_id), 2) AS avg_session_id,
        COUNT(*) AS n_rows
    FROM with_activity
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
)
SELECT
    session_start_month,
    session_id,
    pct_operator_not_in_op_eve_op_be,
    distinct_operators,
    avg_session_id,
    n_rows
FROM grouped
WHERE pct_operator_not_in_op_eve_op_be < 55.0
ORDER BY session_start_month
