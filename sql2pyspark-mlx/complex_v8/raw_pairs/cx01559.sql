WITH cleaned_moves AS (
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
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE unit_price >= 112.8
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        ROUND(SUM(session_qty) / NULLIF(SUM(session_no), 0), 3) AS session_qty_to_session_no_ratio,
        COUNT(*) AS cnt,
        ROUND(SUM(duration_min) / NULLIF(SUM(session_no), 0), 3) AS duration_min_to_session_no_ratio
    FROM session_summary
    WHERE session_no < 4
    GROUP BY move_type
    HAVING MAX(event_count) > 3
)
-- Final projection
SELECT
    move_type,
    session_qty_to_session_no_ratio,
    cnt,
    duration_min_to_session_no_ratio,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
FROM stats
WHERE duration_min_to_session_no_ratio BETWEEN 2.0 AND 8.0
ORDER BY move_type_slug
