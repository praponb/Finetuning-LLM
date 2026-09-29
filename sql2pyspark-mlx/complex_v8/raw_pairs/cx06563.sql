WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(unit_price_bucket) AS session_unit_price_bucket,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_no
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        NTH_VALUE(session_start, 2) OVER (PARTITION BY session_no ORDER BY session_unit_price_bucket, operator, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_start,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY operator), 0), 4) AS session_no_share_of_operator,
        MAX(duration_min) OVER w_operator_2 AS operator_max_duration_min
    FROM per_session
    WINDOW w_operator_2 AS (PARTITION BY operator)
),
derived AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(operator)) AS operator_clean
    FROM sequenced
    WHERE session_unit_price_bucket >= 401
)
-- Final projection
SELECT
    operator,
    session_no,
    session_end,
    events,
    session_unit_price_bucket,
    duration_min,
    rows_in_operator,
    session_no_share_of_operator,
    operator_max_duration_min,
    operator_clean,
    TO_DATE(session_end) AS session_end_date
FROM derived
