WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
),
with_prev AS (
    -- Previous moved_at_month per sku_id
    SELECT *, LAG(moved_at_month) OVER (PARTITION BY sku_id ORDER BY moved_at_month, id) AS prev_moved_at_month
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at_month IS NULL OR (UNIX_TIMESTAMP(moved_at_month) - UNIX_TIMESTAMP(prev_moved_at_month)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        sku_id,
        burst_id,
        MIN(moved_at_month) AS session_start,
        MAX(moved_at_month) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at_month)) - UNIX_TIMESTAMP(MIN(moved_at_month))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, burst_id
)
-- Final projection
SELECT
    sku_id,
    burst_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    DATEDIFF(DATE '2025-06-30', session_start) AS days_since_session_start,
    GREATEST(burst_id, n_events) AS greatest_burst_id_n_events
FROM per_session
ORDER BY session_start DESC
