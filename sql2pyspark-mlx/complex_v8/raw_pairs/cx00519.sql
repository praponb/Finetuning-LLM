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
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
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
labeled AS (
    -- Derive helper columns
    SELECT
        move_type,
        burst_id,
        session_start,
        events,
        duration_min,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        CASE WHEN duration_min >= 156.0 THEN 'A' WHEN duration_min >= 96.0 THEN 'B' ELSE 'C' END AS duration_min_tier,
        ROUND(duration_min * 0.9, 2) AS duration_min_adj
    FROM per_session
    WHERE HOUR(session_end) BETWEEN 9 AND 15
)
-- Final projection
SELECT
    move_type,
    burst_id,
    session_start,
    move_type_group,
    duration_min_tier,
    duration_min_adj
FROM labeled
ORDER BY burst_id DESC, duration_min_tier, move_type
LIMIT 10
