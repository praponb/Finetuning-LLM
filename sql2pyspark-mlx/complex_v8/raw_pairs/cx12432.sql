WITH recent AS (
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
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE sku_id >= 12
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        move_type,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, session_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_events - AVG(n_events) OVER w_n_events_move_type, 2) AS n_events_vs_n_events_move_type_avg,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY n_events, move_type), 2) AS session_id_vs_n_events_move_type_avg,
        FIRST_VALUE(session_start) IGNORE NULLS OVER (PARTITION BY n_events ORDER BY session_end, move_type, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start,
        NTILE(10) OVER (PARTITION BY n_events ORDER BY session_end, move_type, session_id) AS session_end_decile
    FROM session_summary
    WINDOW w_n_events_move_type AS (PARTITION BY n_events, move_type)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE session_end_decile > (SELECT MIN(session_end_decile) FROM metrics)
)
-- Final projection
SELECT
    move_type,
    session_id,
    session_start,
    n_events,
    session_unit_price,
    duration_min,
    n_events_vs_n_events_move_type_avg,
    first_known_session_start,
    session_end_decile,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
FROM with_activity
ORDER BY n_events_vs_n_events_move_type_avg
