WITH filtered AS (
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
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE warehouse_id <= 5
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        sku_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, session_no
),
with_parent_sku AS (
    -- Join skus
    SELECT s.*, sk.weight_kg AS sku_weight_kg
    FROM sessions s
    JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg >= 6.4
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        session_no + session_qty AS session_no_plus_session_qty,
        ROUND(duration_min * 0.9, 2) AS duration_min_adj
    FROM with_parent_sku
    WHERE sku_id <= 10
)
-- Final projection
SELECT
    sku_id,
    session_no,
    session_start,
    event_count,
    session_qty,
    duration_min,
    sku_weight_kg,
    session_no_plus_session_qty,
    duration_min_adj,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start,
    ROUND(sku_weight_kg * 1.1, 2) AS sku_weight_kg_adj
FROM annotated
ORDER BY session_no_plus_session_qty DESC, session_start DESC
