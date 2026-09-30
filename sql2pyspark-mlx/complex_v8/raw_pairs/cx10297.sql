WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CASE WHEN move_type IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE unit_price >= 209.4
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.move_type_group,
        s.move_type_operator_label,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.qty AS move_qty
    FROM scoped s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type = 'OUTBOUND'
),
derived AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(move_operator), 1, 2) AS move_operator_prefix
    FROM line_level_moves
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM derived
    WHERE move_qty < (SELECT MAX(move_qty) FROM derived)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    move_type_group,
    move_type_operator_label,
    move_id,
    move_move_type,
    move_moved_at,
    move_operator,
    move_operator_prefix,
    FLOOR(move_qty / 100) * 100 AS move_qty_bucket
FROM without_matches
ORDER BY warehouse_id DESC, move_moved_at, id, move_id
LIMIT 5
