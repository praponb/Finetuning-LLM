WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        warehouse_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, burst_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        warehouse_id,
        burst_id,
        session_start,
        session_end,
        event_count,
        duration_min,
        SUM(duration_min) OVER (PARTITION BY event_count, YEAR(session_end) ORDER BY session_qty ASC, warehouse_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_min,
        RANK() OVER (PARTITION BY event_count, YEAR(session_end) ORDER BY session_qty ASC) AS session_qty_rank,
        ROUND(PERCENT_RANK() OVER w_warehouse_id_by_duration_min, 3) AS duration_min_percent_rank
    FROM sessions
    WINDOW w_warehouse_id_by_duration_min AS (PARTITION BY warehouse_id ORDER BY duration_min, warehouse_id, burst_id)
)
-- Final projection
SELECT
    warehouse_id,
    burst_id,
    session_start,
    duration_min,
    running_duration_min,
    session_qty_rank,
    duration_min_percent_rank,
    GREATEST(burst_id, duration_min) AS greatest_burst_id_duration_min,
    event_count - session_qty_rank AS event_count_minus_session_qty_rank
FROM metrics
WHERE warehouse_id IS NOT NULL
ORDER BY duration_min DESC, warehouse_id, burst_id
LIMIT 10
