WITH prepared_moves AS (
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
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE unit_price BETWEEN 60.4 AND 396.4
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(qty_filled) AS session_qty_filled,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, burst_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(event_count) OVER (PARTITION BY TO_DATE(session_start), burst_id) AS session_start_to_date_burst_id_max_event_count,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY TO_DATE(session_start), burst_id)) AS distinct_operator_in_session_start_to_date_burst,
        LEAD(session_end) OVER (PARTITION BY operator, burst_id ORDER BY session_start DESC, operator, burst_id) AS next_session_end,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY session_end, operator, burst_id) AS rn,
        ROUND(AVG(burst_id) OVER (PARTITION BY TO_DATE(session_start), burst_id), 2) AS session_start_to_date_burst_id_avg_burst_id
    FROM per_session
),
tagged AS (
    -- Derive helper columns
    SELECT
        operator,
        burst_id,
        session_start,
        session_qty_filled,
        session_start_to_date_burst_id_max_event_count,
        distinct_operator_in_session_start_to_date_burst,
        next_session_end,
        rn,
        session_start_to_date_burst_id_avg_burst_id,
        ABS(rn - 4) AS rn_distance,
        CONCAT(operator, '#', CAST(session_qty_filled AS STRING)) AS operator_ref
    FROM windowed
)
SELECT
    operator,
    burst_id,
    session_start,
    session_qty_filled,
    distinct_operator_in_session_start_to_date_burst,
    next_session_end,
    session_start_to_date_burst_id_avg_burst_id,
    rn_distance,
    DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start
FROM tagged
ORDER BY session_start_to_date_burst_id_avg_burst_id DESC, session_start
