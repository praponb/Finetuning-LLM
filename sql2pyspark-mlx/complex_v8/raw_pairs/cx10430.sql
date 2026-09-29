WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY session_id, n_events)) AS distinct_move_type_in_session_id_n_events,
        COUNT(*) OVER (PARTITION BY session_id, n_events) AS rows_in_session_id_n_events
    FROM per_session
),
grouped AS (
    -- Group-level totals
    SELECT
        TO_DATE(session_start) AS session_start_day,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        MIN(session_id) AS min_session_id
    FROM with_windows
    GROUP BY 1
)
-- Final projection
SELECT session_start_day, move_type_list
FROM grouped
WHERE session_start_day IS NOT NULL
ORDER BY session_start_day DESC
LIMIT 10
