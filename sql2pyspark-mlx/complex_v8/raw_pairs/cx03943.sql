WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM valid_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Subtotals with CUBE
SELECT
    move_id,
    COALESCE(move_type, 'ALL') AS move_type,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS n_move_type_is_not_null,
    ROUND(STDDEV(move_unit_price), 2) AS stddev_move_unit_price
FROM line_level_moves
GROUP BY CUBE(move_id, move_type)
