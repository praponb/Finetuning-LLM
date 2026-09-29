WITH valid AS (
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
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        sku_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, session_id
)
-- Final projection
SELECT
    sku_id,
    session_start,
    session_end,
    n_events
FROM per_session
ORDER BY n_events
