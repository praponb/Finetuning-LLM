WITH cleaned AS (
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
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE move_type NOT IN ('INBOUND', 'ADJUST')
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(least_unit_price_qty) AS session_least_unit_price_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        warehouse_id,
        session_id,
        session_end,
        n_events,
        duration_min,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end,
        DATE_FORMAT(session_end, 'MMM yyyy') AS session_end_label,
        ABS(session_id - 3) AS session_id_distance
    FROM sessions
)
SELECT
    warehouse_id,
    session_end,
    n_events,
    duration_min,
    hours_session_start_to_session_end,
    session_end_label,
    session_id_distance,
    LENGTH(TRIM(session_end_label)) AS session_end_label_len,
    LENGTH(TRIM(session_end_label)) AS session_end_label_len_2
FROM labeled
ORDER BY hours_session_start_to_session_end, n_events
