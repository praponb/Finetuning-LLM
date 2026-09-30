WITH prepared AS (
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
        CASE WHEN unit_price >= 328.3 THEN 'HIGH' WHEN unit_price >= 238.5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM prepared s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.moved_at >= TIMESTAMP '2025-01-31 10:00:00'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT operator, move_type, qty
    FROM qualified
) src
PIVOT (
    COUNT(qty)
    FOR move_type IN ('TRANSFER', 'OUTBOUND', 'ADJUST', 'INBOUND')
)
ORDER BY operator
