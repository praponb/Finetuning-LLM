WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 13
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
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
    events,
    duration_min,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM sessions
ORDER BY operator
