WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
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
    WHERE unit_price <= 398.5
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_no
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(move_type)) AS move_type_clean,
        DATEDIFF(DATE '2026-01-01', session_end) AS days_since_session_end
    FROM per_session
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(move_type_clean)) AS move_type_clean_len,
        CASE WHEN session_no >= 5 THEN 'HIGH' WHEN session_no >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS session_no_tier,
        FLOOR(days_since_session_end / 100) * 100 AS days_since_session_end_bucket
    FROM enriched
)
-- Final projection
SELECT
    move_type,
    session_no,
    session_start,
    session_end,
    events,
    duration_min,
    move_type_clean,
    days_since_session_end,
    move_type_clean_len,
    session_no_tier,
    days_since_session_end_bucket,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 3600, 1) AS hours_session_end_to_session_start
FROM scored
ORDER BY session_no_tier, events
