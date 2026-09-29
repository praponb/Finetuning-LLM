WITH cleaned AS (
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
    WHERE operator IN ('op_bee', 'op_ann')
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE events > (SELECT AVG(events) FROM sessions)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE events < (SELECT MAX(events) FROM screened)
)
SELECT
    move_type,
    session_start,
    events,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
FROM filtered
ORDER BY events, session_start DESC NULLS FIRST
