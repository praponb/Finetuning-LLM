WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prev_moved_at,
    starts_session,
    session_id,
    qty - starts_session AS qty_minus_starts_session
FROM sessionized
ORDER BY moved_at, id
LIMIT 5
