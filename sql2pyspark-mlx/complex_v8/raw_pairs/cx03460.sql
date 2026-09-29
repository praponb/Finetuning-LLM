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
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, burst_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        operator,
        burst_id,
        session_start,
        session_end,
        events,
        ROUND(PERCENT_RANK() OVER (ORDER BY session_end), 3) AS session_end_percent_rank,
        events - LAG(events) OVER w_session_start_to_date_by_ses AS events_change,
        SUM(burst_id) OVER (ORDER BY session_end, operator, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
        ROUND(CUME_DIST() OVER w_all_by_session_end_ties, 3) AS session_end_cume_dist,
        LEAD(burst_id, 1, 0) OVER (ORDER BY session_end, operator, burst_id) AS next_burst_id,
        COUNT(*) OVER (PARTITION BY events) AS rows_in_events
    FROM per_session
    WINDOW w_session_start_to_date_by_ses AS (PARTITION BY TO_DATE(session_start) ORDER BY session_start NULLS LAST, operator, burst_id), w_all_by_session_end_ties AS (ORDER BY session_end)
)
-- Final projection
SELECT
    operator,
    burst_id,
    session_end,
    session_end_percent_rank,
    events_change,
    running_burst_id,
    session_end_cume_dist,
    rows_in_events,
    DATE_TRUNC('DAY', session_end) AS session_end_day
FROM with_trends
ORDER BY events_change, session_end_day
