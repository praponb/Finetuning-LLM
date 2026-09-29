WITH cleaned AS (
    -- Scope the stock_moves rows
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
    WHERE unit_price >= 150.7
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.prev_moved_at,
        s.starts_session,
        st.id AS move_id,
        st.operator AS move_operator,
        st.move_type AS move_move_type,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty
    FROM sessionized s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price,
    prev_moved_at,
    starts_session,
    move_id,
    move_move_type,
    move_warehouse_id,
    move_qty
FROM exploded_detail_moves
ORDER BY starts_session NULLS LAST, move_qty DESC, id, move_id
LIMIT 5
