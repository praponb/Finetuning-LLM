WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE warehouse_id <= 5
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    unit_price,
    prev_moved_at,
    new_session_flag
FROM sessionized
ORDER BY new_session_flag DESC, id
LIMIT 20
