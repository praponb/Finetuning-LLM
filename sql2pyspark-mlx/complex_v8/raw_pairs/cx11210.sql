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
        ABS(qty - 230) AS qty_distance
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        is_new_session + unit_price AS is_new_session_plus_unit_price,
        CASE WHEN move_type IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND(is_new_session / NULLIF(unit_price, 0), 3) AS is_new_session_per_unit_price
    FROM sessionized
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    prev_moved_at,
    is_new_session,
    session_no,
    is_new_session_plus_unit_price,
    move_type_group,
    is_new_session_per_unit_price,
    DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
    CONCAT_WS(': ', move_type_group, move_type) AS move_type_group_move_type_label
FROM enriched
ORDER BY warehouse_id DESC, move_type_group_move_type_label
