WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM base_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
      AND s.warehouse_id IS NOT NULL
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    prev_moved_at,
    is_new_session,
    burst_id,
    TO_DATE(prev_moved_at) AS prev_moved_at_date
FROM without_matches
