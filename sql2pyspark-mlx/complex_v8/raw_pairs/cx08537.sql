WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-06 12:00:00'
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM base_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY session_id)) AS distinct_warehouse_id_in_session_id,
        LAG(duration_min, 3) OVER w_warehouse_id_by_session_qty AS prev3_duration_min,
        MIN(duration_min) OVER (PARTITION BY session_id) AS session_id_min_duration_min,
        LEAD(session_start) OVER (PARTITION BY warehouse_id ORDER BY session_qty, warehouse_id, session_id) AS next_session_start
    FROM per_session
    WINDOW w_warehouse_id_by_session_qty AS (PARTITION BY warehouse_id ORDER BY session_qty, warehouse_id, session_id)
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty >= 148
        )
)
SELECT
    warehouse_id,
    session_id,
    session_start,
    session_end,
    n_events,
    session_qty,
    duration_min,
    distinct_warehouse_id_in_session_id,
    session_id_min_duration_min,
    ROUND(prev3_duration_min * 0.25, 2) AS prev3_duration_min_adj,
    DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start
FROM filtered
ORDER BY session_end, n_events
