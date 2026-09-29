WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE qty > 145
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM prepared s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count, COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT *
FROM (
    SELECT warehouse_opened_on, move_type, operator, qty
    FROM with_counts
) src
PIVOT (
    COUNT(qty) AS count_v, SUM(qty) AS sum_v
    FOR operator IN ('op_ann', 'op_eve', 'op_bee', 'op_dew')
)
