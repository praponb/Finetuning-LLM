WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        warehouse_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, burst_id
),
labeled AS (
    -- Label and score rows
    SELECT
        warehouse_id,
        burst_id,
        session_start,
        session_end,
        session_qty,
        duration_min,
        COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM sessions
    WHERE n_events BETWEEN 2 AND 4
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        session_start + INTERVAL 2 HOURS AS session_start_plus_2h,
        DATE_FORMAT(session_start, 'MMM yyyy') AS session_start_label
    FROM labeled
    WHERE warehouse_id IS NOT NULL
)
SELECT
    warehouse_id,
    burst_id,
    session_end,
    session_qty,
    duration_min,
    duration_min_filled,
    session_start_plus_2h
FROM enriched
WHERE burst_id > 2
ORDER BY warehouse_id NULLS LAST
