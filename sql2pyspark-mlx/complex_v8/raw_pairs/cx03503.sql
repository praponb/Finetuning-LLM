WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
    WHERE qty >= 189
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
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
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(session_start, 'yyyy-MM-dd HH:00') AS session_start_label,
        CASE WHEN burst_id >= 4 THEN 'HIGH' WHEN burst_id >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS burst_id_tier,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM sessions
),
labeled AS (
    -- Add calculated columns
    SELECT
        move_type,
        burst_id,
        session_end,
        n_events,
        session_qty,
        burst_id_tier,
        CONCAT(move_type, '#', CAST(session_qty AS STRING)) AS move_type_ref
    FROM annotated
    WHERE move_type = 'ADJUST'
)
SELECT
    move_type,
    burst_id,
    session_end,
    n_events,
    session_qty,
    burst_id_tier,
    move_type_ref
FROM labeled
ORDER BY move_type, move_type_ref DESC
