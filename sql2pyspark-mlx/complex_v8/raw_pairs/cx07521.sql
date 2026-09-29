WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, burst_id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM per_session
    WHERE session_qty > (SELECT MIN(session_qty) FROM per_session)
),
tagged AS (
    -- Label and score rows
    SELECT *, TO_DATE(session_end) AS session_end_date
    FROM with_activity
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT burst_id, session_start, move_type, duration_min
    FROM tagged
) src
PIVOT (
    SUM(duration_min)
    FOR move_type IN ('ADJUST', 'OUTBOUND', 'INBOUND', 'TRANSFER')
)
