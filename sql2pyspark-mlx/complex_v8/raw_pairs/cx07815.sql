WITH scoped_moves AS (
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
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-02'
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        move_type,
        session_no,
        session_start,
        session_end,
        event_count,
        duration_min,
        NTH_VALUE(event_count, 2) OVER (PARTITION BY event_count, move_type ORDER BY session_start, move_type, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_count,
        MAX(event_count) OVER (PARTITION BY event_count, move_type ORDER BY session_start, move_type, session_no ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_count_max_6_prior,
        SUM(session_no) OVER (PARTITION BY TO_DATE(session_end), YEAR(session_start)) AS session_end_to_date_session_start_year_sum_sessi
    FROM per_session
),
derived AS (
    -- Add calculated columns
    SELECT
        move_type,
        session_no,
        session_end,
        event_count,
        event_count_max_6_prior,
        session_end_to_date_session_start_year_sum_sessi,
        CONCAT(move_type, '#', CAST(event_count AS STRING)) AS move_type_ref,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM with_windows
    WHERE MONTH(session_end) IN (2, 3)
)
SELECT
    move_type,
    session_no,
    session_end,
    event_count,
    event_count_max_6_prior,
    session_end_to_date_session_start_year_sum_sessi,
    move_type_ref,
    move_type_len,
    DATEDIFF(DATE '2025-12-31', session_end) AS days_since_session_end
FROM derived
ORDER BY session_no, move_type NULLS LAST
