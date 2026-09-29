WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, burst_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT AVG(duration_min) FROM per_session)
)
SELECT
    move_type,
    burst_id,
    session_start,
    session_end,
    events,
    session_unit_price,
    duration_min,
    DATE_FORMAT(session_start, 'yyyy-MM-dd') AS session_start_label
FROM without_matches
ORDER BY burst_id DESC NULLS FIRST, move_type
LIMIT 20
