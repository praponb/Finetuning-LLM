WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.prev_moved_at,
        s.new_session_flag,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM sessionized s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type <> 'OUTBOUND'
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        st.last_moved_at
    FROM detail_rows s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at,
    operator,
    unit_price,
    prev_moved_at,
    new_session_flag,
    move_id,
    move_move_type,
    move_operator,
    move_unit_price,
    avg_move_unit_price
FROM profiled
ORDER BY id NULLS LAST, move_id
LIMIT 5
