WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
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
    FROM prepared_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_activity AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.category = 'Food'
              AND LOWER(sk.sku_code) LIKE '%-4%'
        )
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    prev_moved_at,
    is_new_session,
    session_no
FROM with_activity
ORDER BY sku_id DESC, is_new_session NULLS LAST
