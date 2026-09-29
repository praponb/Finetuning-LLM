WITH scoped_moves AS (
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
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        sku_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, session_id
),
stats AS (
    -- Group-level totals
    SELECT
        sku_id,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        COUNT(*) AS n_rows,
        MIN(duration_min) AS min_duration_min,
        SUM(session_id) AS total_session_id,
        MIN(events) AS min_events,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids
    FROM sessions
    GROUP BY sku_id, DATE_TRUNC('MONTH', session_end), DATE_TRUNC('MONTH', session_start)
    HAVING COUNT(*) >= 2
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        session_end_month + INTERVAL 7 DAYS AS session_end_month_plus_7d,
        ROUND(total_session_id / NULLIF(min_events, 0), 3) AS total_session_id_per_min_events
    FROM stats
    WHERE n_rows > 2
)
SELECT
    sku_id,
    session_start_month,
    n_rows,
    min_duration_min,
    min_events,
    distinct_sku_ids,
    session_end_month_plus_7d,
    total_session_id_per_min_events,
    CASE WHEN min_duration_min >= 132.0 THEN 'large' WHEN min_duration_min >= 48.0 THEN 'medium' ELSE 'small' END AS min_duration_min_tier
FROM derived
WHERE min_events < 5
ORDER BY min_duration_min_tier DESC, sku_id DESC
