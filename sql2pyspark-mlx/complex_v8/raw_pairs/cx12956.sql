WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ABS(unit_price - 290.4) AS unit_price_distance
    FROM stock_moves
    WHERE unit_price > 86.3
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type
    FROM valid_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, move_type, move_unit_price
    FROM with_children_moves
) src
PIVOT (
    SUM(move_unit_price)
    FOR move_type IN ('TRANSFER', 'INBOUND', 'ADJUST')
)
