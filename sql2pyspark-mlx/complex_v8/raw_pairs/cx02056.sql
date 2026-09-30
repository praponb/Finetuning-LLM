WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE qty BETWEEN 90 AND 384
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    unit_price,
    qty_minus_unit_price,
    prev_moved_at,
    session_id,
    ROUND(unit_price / NULLIF(is_new_session, 0), 3) AS unit_price_per_is_new_session,
    DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
FROM sessionized
WHERE sku_id >= 10
ORDER BY days_since_moved_at DESC
