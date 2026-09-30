WITH cleaned AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE operator = 'op_ann'
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        warehouse_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, burst_id
),
enriched AS (
    -- Add calculated columns
    SELECT *, COALESCE(n_events, 0) AS n_events_filled
    FROM per_session
    WHERE session_end >= TIMESTAMP '2025-01-21 09:00:00'
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM enriched s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
SELECT
    warehouse_id,
    session_start,
    n_events,
    move_count,
    CASE WHEN n_events_filled >= 4 THEN 'HIGH' WHEN n_events_filled >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_events_filled_tier,
    DATE_TRUNC('YEAR', session_end) AS session_end_year
FROM with_counts
ORDER BY n_events_filled_tier
