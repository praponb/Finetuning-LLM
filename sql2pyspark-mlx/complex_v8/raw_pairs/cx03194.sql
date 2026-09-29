WITH filtered AS (
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
    WHERE move_type IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_no
),
derived AS (
    -- Label and score rows
    SELECT
        operator,
        session_no,
        session_end,
        n_events,
        session_qty,
        duration_min,
        CASE WHEN session_qty >= 1300 THEN 'large' WHEN session_qty >= 501 THEN 'medium' ELSE 'small' END AS session_qty_tier,
        ROUND(session_no / NULLIF(duration_min, 0), 3) AS session_no_per_duration_min
    FROM session_summary
),
latest_per_key AS (
    -- Keep the latest row per session_qty_tier
    SELECT
        operator,
        session_no,
        session_end,
        n_events,
        session_qty,
        duration_min,
        session_qty_tier,
        session_no_per_duration_min
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY session_qty_tier ORDER BY duration_min DESC, operator DESC, session_no DESC) AS rn
        FROM derived
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    operator,
    session_no,
    session_end,
    session_qty,
    duration_min,
    session_qty_tier,
    LEAST(n_events, duration_min) AS least_n_events_duration_min
FROM latest_per_key
ORDER BY least_n_events_duration_min NULLS LAST, session_qty NULLS LAST
