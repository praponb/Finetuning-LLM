WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE operator NOT IN ('op_eve', 'op_dew')
      AND TO_DATE(moved_at) < DATE '2025-02-06'
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND MONTH(st.moved_at) IN (1, 3)
        )
      AND TO_DATE(s.moved_at) < DATE '2025-02-06'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, move_type, qty
    FROM filtered
) src
PIVOT (
    SUM(qty)
    FOR move_type IN ('INBOUND', 'OUTBOUND', 'ADJUST')
)
ORDER BY sku_id
