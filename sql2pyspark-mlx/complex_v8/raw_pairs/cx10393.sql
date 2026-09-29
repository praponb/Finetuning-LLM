WITH prepared_moves AS (
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
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
tagged AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM session_summary
),
enriched AS (
    -- Label and score rows
    SELECT
        operator,
        session_id,
        session_end,
        duration_min,
        operator_prefix,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket,
        DATEDIFF(DATE '2025-06-30', session_start) AS days_since_session_start
    FROM tagged
)
-- Final projection
SELECT
    operator,
    session_id,
    duration_min,
    duration_min_bucket,
    days_since_session_start,
    UPPER(TRIM(operator)) AS operator_clean
FROM enriched
ORDER BY session_id DESC, days_since_session_start DESC
