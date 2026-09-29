WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator = 'op_chan'
        )
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_activity s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type <> 'TRANSFER'
        )
)
SELECT id, measure, value
FROM (
    SELECT id, starts_session, burst_id, days_since_moved_at
    FROM filtered
) src
UNPIVOT (
    value FOR measure IN (starts_session, burst_id, days_since_moved_at)
) unp
ORDER BY id, measure
