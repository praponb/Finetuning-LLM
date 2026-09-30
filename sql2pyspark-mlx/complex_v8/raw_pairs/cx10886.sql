WITH recent_moves AS (
    -- Scope the stock_moves rows
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
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM recent_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        sku_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, burst_id
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.sku_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.event_count,
        s.duration_min,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM session_summary s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.is_active = TRUE
)
SELECT
    sku_id,
    burst_id,
    session_start,
    session_end,
    sku_is_active,
    sku_unit_cost,
    sku_weight_kg
FROM joined_sku
ORDER BY sku_unit_cost DESC
