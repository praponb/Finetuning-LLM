WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE operator = 'op_ann'
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_no
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.warehouse_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.session_qty,
        s.duration_min,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity
    FROM sessions s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    warehouse_id,
    session_start,
    session_end,
    session_qty,
    duration_min,
    warehouse_city,
    warehouse_capacity,
    COALESCE(warehouse_city, 'UNKNOWN') AS warehouse_city_or_unknown,
    session_no - duration_min AS session_no_minus_duration_min
FROM combined_details_warehouse
ORDER BY session_start, session_qty
