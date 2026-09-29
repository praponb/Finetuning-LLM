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
        unit_price,
        ABS(qty - 230) AS qty_distance
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        operator,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, burst_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY burst_id ORDER BY n_events, operator, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(n_events) OVER (PARTITION BY n_events, burst_id) AS n_events_burst_id_min_n_events,
        ROUND(AVG(n_events) OVER (PARTITION BY burst_id ORDER BY n_events, operator, burst_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS n_events_avg_3_prior
    FROM sessions
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE running_count > (SELECT AVG(running_count) FROM sequenced)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_start, session_end, operator, n_events
    FROM with_activity
) src
PIVOT (
    SUM(n_events)
    FOR operator IN ('op_bee', 'op_ann', 'op_eve', 'op_dew')
)
ORDER BY session_start, session_end
