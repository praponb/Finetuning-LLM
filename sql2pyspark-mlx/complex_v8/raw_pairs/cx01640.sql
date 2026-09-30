WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE unit_price > 61.2
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id
    FROM scoped s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id <= 7
)
SELECT *
FROM (
    SELECT move_sku_id, move_type, qty
    FROM detail_rows
) src
PIVOT (
    AVG(qty)
    FOR move_type IN ('INBOUND', 'TRANSFER', 'ADJUST', 'OUTBOUND')
)
ORDER BY move_sku_id
