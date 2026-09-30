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
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
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
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end
    FROM per_session
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_count,
        operator,
        session_id,
        ROUND(STDDEV(hours_session_start_to_session_end), 2) AS stddev_hours_session_start_to_session_end,
        ROUND(SUM(session_id) / NULLIF(SUM(event_count), 0), 3) AS session_id_to_event_count_ratio,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        MIN(session_end) AS first_session_end,
        SUM(event_count) AS total_event_count,
        COUNT(*) AS cnt
    FROM labeled
    GROUP BY event_count, operator, session_id
    HAVING COUNT(*) >= 2
)
SELECT
    event_count,
    operator,
    stddev_hours_session_start_to_session_end,
    session_id_to_event_count_ratio,
    operator_list,
    first_session_end,
    total_event_count,
    cnt
FROM stats
WHERE SIZE(operator_list) >= 1
ORDER BY total_event_count, cnt DESC NULLS FIRST
