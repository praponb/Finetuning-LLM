WITH scoped_moves AS (
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
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE unit_price > 164.1
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM scoped_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
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
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE session_id > (SELECT AVG(session_id) FROM per_session)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end_2,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM screened
)
SELECT
    operator,
    session_id,
    session_start,
    session_end,
    session_unit_price,
    duration_min,
    minutes_session_start_to_session_end,
    minutes_session_start_to_session_end_2,
    operator_prefix,
    ROUND(session_unit_price * 0.25, 2) AS session_unit_price_adj,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM labeled
ORDER BY session_start, minutes_session_start_to_session_end NULLS LAST
