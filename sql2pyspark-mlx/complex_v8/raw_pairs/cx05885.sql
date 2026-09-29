WITH recent_moves AS (
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
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
per_group AS (
    -- Summarise per group
    SELECT
        events,
        MAX(session_end) AS last_session_end,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        MIN(session_start) AS first_session_start
    FROM session_summary
    GROUP BY 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        events,
        last_session_end,
        first_session_start,
        DATEDIFF(DATE '2026-01-01', last_session_end) AS days_since_last_session_end,
        ABS(distinct_move_types - 6) AS distinct_move_types_distance,
        ROUND((UNIX_TIMESTAMP(first_session_start) - UNIX_TIMESTAMP(last_session_end)) / 3600, 1) AS hours_last_session_end_to_first_session_start
    FROM per_group
)
-- Final projection
SELECT
    events,
    last_session_end,
    first_session_start,
    days_since_last_session_end,
    distinct_move_types_distance,
    hours_last_session_end_to_first_session_start,
    first_session_start + INTERVAL 15 MINUTES AS first_session_start_plus_15m
FROM tagged
ORDER BY last_session_end
