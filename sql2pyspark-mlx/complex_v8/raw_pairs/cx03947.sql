WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE operator NOT IN ('op_eve', 'op_ann')
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM sessions
    WHERE session_qty < (SELECT MAX(session_qty) FROM sessions)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        operator,
        session_id,
        session_end,
        events,
        session_qty,
        duration_min,
        LAG(duration_min) OVER w_all_by_session_start AS prev_duration_min,
        SIZE(COLLECT_SET(operator) OVER w_session_id) AS distinct_operator_in_session_id,
        events - LAG(events) OVER (PARTITION BY TO_DATE(session_end) ORDER BY session_start DESC, operator, session_id) AS events_change
    FROM without_matches
    WINDOW w_all_by_session_start AS (ORDER BY session_start, operator, session_id), w_session_id AS (PARTITION BY session_id)
)
SELECT
    operator,
    session_end,
    events,
    session_qty,
    duration_min,
    prev_duration_min,
    distinct_operator_in_session_id,
    events_change,
    DAYOFWEEK(session_end) AS session_end_dayofweek,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM windowed
ORDER BY operator, operator_slug DESC NULLS FIRST
