WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 3
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM scoped_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type IN ('OUTBOUND', 'TRANSFER', 'ADJUST')
              AND st.qty < 422
        )
      AND s.sku_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, sku_id, move_type, unit_price
    FROM qualified
) src
PIVOT (
    AVG(unit_price) AS avg_v, COUNT(unit_price) AS n
    FOR move_type IN ('INBOUND', 'OUTBOUND', 'ADJUST', 'TRANSFER')
)
