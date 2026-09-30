WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
per_group AS (
    -- Summarise per group
    SELECT
        move_type,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_ym,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        MIN(session_end) AS first_session_end,
        MAX(event_count) AS max_event_count,
        ROUND(SUM(session_no) / NULLIF(SUM(event_count), 0), 3) AS session_no_to_event_count_ratio,
        COUNT(*) FILTER (WHERE move_type <> 'TRANSFER') AS n_move_type_transfer
    FROM session_summary
    GROUP BY move_type, DATE_FORMAT(session_end, 'yyyy-MM'), DATE_TRUNC('MONTH', session_start)
)
-- Final projection
SELECT
    move_type,
    first_session_end,
    max_event_count,
    session_no_to_event_count_ratio,
    CASE WHEN move_type IN ('OUTBOUND', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
    max_event_count + n_move_type_transfer AS max_event_count_plus_n_move_type_transfer
FROM per_group
WHERE n_move_type_transfer > 1
ORDER BY max_event_count DESC NULLS FIRST
