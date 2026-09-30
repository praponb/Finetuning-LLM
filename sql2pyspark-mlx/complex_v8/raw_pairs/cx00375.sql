WITH base AS (
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
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE qty <= 288
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_no
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        warehouse_id,
        session_no,
        session_start,
        session_end,
        duration_min,
        ROUND(AVG(duration_min) OVER (PARTITION BY YEAR(session_start), session_no), 2) AS session_start_year_session_no_avg_duration_min,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY YEAR(session_start), session_no), 0), 4) AS session_no_share_of_session_start_year_session_n,
        ROUND(AVG(duration_min) OVER (ORDER BY session_end DESC, warehouse_id, session_no ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_2
    FROM per_session
)
-- Final projection
SELECT
    warehouse_id,
    session_start,
    session_end,
    duration_min,
    session_start_year_session_no_avg_duration_min,
    FLOOR(duration_min / 50) * 50 AS duration_min_bucket
FROM with_windows
ORDER BY session_start_year_session_no_avg_duration_min DESC
