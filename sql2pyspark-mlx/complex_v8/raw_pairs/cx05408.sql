WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek,
        CASE WHEN operator IN ('op_ann', 'op_chan') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE unit_price >= 112.8
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_group,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM filtered_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.operator IN ('op_dew', 'op_chan')
)
SELECT *
FROM (
    SELECT move_id, move_type, move_unit_price
    FROM with_children_moves
) src
PIVOT (
    COUNT(move_unit_price)
    FOR move_type IN ('INBOUND', 'ADJUST', 'TRANSFER', 'OUTBOUND')
)
ORDER BY move_id
