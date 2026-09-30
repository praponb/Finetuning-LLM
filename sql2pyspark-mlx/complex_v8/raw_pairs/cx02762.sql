WITH filtered AS (
    -- Filter stock_moves
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
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        sku_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, burst_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(LAG(session_end) OVER w_sku_id_by_n_events)) / 60, 1) AS minutes_since_prev_session_end,
        COUNT(*) OVER (PARTITION BY n_events) AS rows_in_n_events,
        ROUND(AVG(burst_id) OVER (PARTITION BY sku_id ORDER BY n_events ASC, sku_id, burst_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS burst_id_avg_4,
        NTILE(3) OVER (PARTITION BY sku_id ORDER BY n_events ASC, sku_id, burst_id) AS n_events_tercile,
        MAX(duration_min) OVER (PARTITION BY n_events) AS n_events_max_duration_min
    FROM sessions
    WINDOW w_sku_id_by_n_events AS (PARTITION BY sku_id ORDER BY n_events ASC, sku_id, burst_id), w_n_events AS (PARTITION BY n_events)
)
SELECT
    sku_id,
    session_start,
    session_end,
    duration_min,
    minutes_since_prev_session_end,
    rows_in_n_events,
    burst_id_avg_4,
    n_events_tercile,
    n_events_max_duration_min
FROM ranked
ORDER BY session_start DESC
