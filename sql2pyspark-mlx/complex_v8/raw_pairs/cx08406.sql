WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(event_count) OVER w_event_count_session_start_to, 2) AS event_count_session_start_to_date_avg_event_coun,
        ROW_NUMBER() OVER (ORDER BY session_start, operator, session_id) AS seq_no,
        LAG(event_count, 2) OVER (PARTITION BY operator ORDER BY duration_min DESC, operator, session_id) AS prev2_event_count,
        event_count - LAG(event_count) OVER (PARTITION BY operator, DATE_TRUNC('MONTH', session_end) ORDER BY session_end, operator, session_id) AS event_count_change
    FROM per_session
    WINDOW w_event_count_session_start_to AS (PARTITION BY event_count, TO_DATE(session_start))
)
-- Final projection
SELECT
    operator,
    session_id,
    session_start,
    duration_min,
    prev2_event_count,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM metrics
