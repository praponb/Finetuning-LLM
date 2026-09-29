WITH scoped_moves AS (
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
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_minus_unit_price,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.warehouse_id AS move_warehouse_id,
        st.move_type AS move_move_type
    FROM scoped_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.move_type = 'TRANSFER'
)
SELECT *
FROM (
    SELECT move_move_type, move_moved_at, operator, unit_price
    FROM exploded_detail_moves
) src
PIVOT (
    MAX(unit_price)
    FOR operator IN ('op_dew', 'op_ann')
)
ORDER BY move_move_type, move_moved_at
