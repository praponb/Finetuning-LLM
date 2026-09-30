WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        move_type,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, burst_id
),
summary AS (
    -- Group-level totals
    SELECT
        burst_id,
        events,
        move_type,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        MIN(session_start) AS first_session_start,
        COUNT(*) AS row_count,
        ROUND(STDDEV(burst_id), 2) AS stddev_burst_id,
        COUNT(*) FILTER (WHERE move_type IN ('OUTBOUND', 'INBOUND', 'ADJUST')) AS n_move_type_in_outbound_inboun
    FROM session_summary
    WHERE move_type = 'INBOUND'
    GROUP BY burst_id, events, move_type
)
SELECT
    burst_id,
    move_type_list,
    first_session_start,
    stddev_burst_id,
    n_move_type_in_outbound_inboun
FROM summary
ORDER BY first_session_start
