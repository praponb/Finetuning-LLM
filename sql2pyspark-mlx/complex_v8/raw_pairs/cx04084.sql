WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM sessionized s
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    operator,
    unit_price,
    move_type_prefix,
    prev_moved_at,
    session_no,
    last_moved_at,
    total_move_qty,
    TO_DATE(last_moved_at) AS last_moved_at_date,
    GREATEST(move_count, session_no) AS greatest_move_count_session_no
FROM profiled
ORDER BY unit_price
