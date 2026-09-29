WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
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
sequenced AS (
    -- Running and moving aggregates
    SELECT
        move_type,
        burst_id,
        session_start,
        session_unit_price,
        duration_min,
        RANK() OVER (ORDER BY session_start DESC) AS session_start_rank,
        MIN(burst_id) OVER (PARTITION BY move_type, YEAR(session_end)) AS move_type_session_end_year_min_burst_id,
        ROW_NUMBER() OVER (PARTITION BY events ORDER BY events DESC NULLS FIRST, move_type, burst_id) AS events_seq,
        ROUND(duration_min - AVG(duration_min) OVER (PARTITION BY move_type, YEAR(session_end)), 2) AS duration_min_vs_move_type_session_end_year_avg
    FROM per_session
)
-- Final projection
SELECT
    move_type,
    burst_id,
    session_start,
    session_unit_price,
    duration_min,
    session_start_rank,
    move_type_session_end_year_min_burst_id,
    events_seq,
    duration_min_vs_move_type_session_end_year_avg
FROM sequenced
ORDER BY duration_min, session_start, move_type, burst_id
LIMIT 5
