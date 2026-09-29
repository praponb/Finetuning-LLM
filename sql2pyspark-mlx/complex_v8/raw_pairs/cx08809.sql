WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_no
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(qty) AS min_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.min_move_qty
    FROM session_summary s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.capacity AS warehouse_capacity, w.wh_code AS warehouse_wh_code
    FROM summarized_rows s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    warehouse_id,
    session_no,
    events,
    min_move_qty,
    warehouse_wh_code,
    YEAR(session_start) AS session_start_year
FROM enriched_warehouse
WHERE duration_min > 84.0
ORDER BY min_move_qty, warehouse_id DESC
