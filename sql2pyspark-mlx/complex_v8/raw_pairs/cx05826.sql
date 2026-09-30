WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price < 292.3
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator_or_unknown,
        s.move_type_ref,
        st.id AS move_id,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id
    FROM cleaned s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    move_operator,
    move_warehouse_id,
    COUNT(*) FILTER (WHERE move_operator IN ('op_ann', 'op_bee')) AS n_move_operator_in_op_ann_op_b
FROM line_level
GROUP BY GROUPING SETS ((move_operator, move_warehouse_id), (move_operator), (move_warehouse_id))
ORDER BY move_operator NULLS LAST, move_warehouse_id NULLS LAST
