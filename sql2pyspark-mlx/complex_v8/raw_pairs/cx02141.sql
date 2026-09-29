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
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        warehouse_id,
        session_id,
        session_start,
        session_end,
        event_count,
        duration_min,
        FIRST_VALUE(event_count) OVER (PARTITION BY MONTH(session_end) ORDER BY duration_min DESC, warehouse_id, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_count,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY event_count, session_id)) AS distinct_warehouse_id_in_event_count_session_id
    FROM session_summary
)
SELECT
    warehouse_id,
    session_id,
    session_start,
    session_end,
    duration_min,
    distinct_warehouse_id_in_event_count_session_id,
    GREATEST(session_id, event_count) AS greatest_session_id_event_count
FROM metrics
WHERE warehouse_id IS NOT NULL
ORDER BY warehouse_id NULLS LAST
