WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-25 10:00:00'
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        operator,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, burst_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        operator,
        burst_id,
        session_start,
        event_count,
        session_qty,
        duration_min,
        LEAD(burst_id, 2) OVER (PARTITION BY YEAR(session_end), burst_id ORDER BY event_count ASC, operator, burst_id) AS next2_burst_id,
        FIRST_VALUE(session_qty) OVER (ORDER BY session_qty ASC, operator, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_qty,
        ROUND(AVG(event_count) OVER w_session_start_month, 2) AS session_start_month_avg_event_count,
        LEAD(session_qty) OVER (ORDER BY session_qty ASC, operator, burst_id) AS next_session_qty,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER (PARTITION BY DATE_TRUNC('MONTH', session_start)), 0), 4) AS duration_min_share_of_session_start_month
    FROM sessions
    WINDOW w_session_start_month AS (PARTITION BY DATE_TRUNC('MONTH', session_start))
)
SELECT
    operator,
    burst_id,
    session_start,
    event_count,
    session_qty,
    duration_min,
    next2_burst_id,
    first_session_qty,
    session_start_month_avg_event_count,
    next_session_qty,
    duration_min_share_of_session_start_month,
    DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM with_trends
WHERE next_session_qty < 1100
ORDER BY operator, session_qty DESC NULLS FIRST
