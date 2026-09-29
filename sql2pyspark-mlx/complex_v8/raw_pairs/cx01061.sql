WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator_len,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM base s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
without_matches AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM exploded_detail s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.move_sku_id
              AND sk.category = 'Apparel'
        )
),
with_details_move AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator_len,
        s.move_id,
        s.move_moved_at,
        s.move_operator,
        s.move_sku_id,
        s.move_qty,
        st.qty AS move_qty_2
    FROM without_matches s
    JOIN stock_moves st ON st.id = s.move_id
    WHERE st.qty <= 349
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT operator_len, sku_id, move_operator, move_qty_2
    FROM with_details_move
) src
PIVOT (
    AVG(move_qty_2)
    FOR move_operator IN ('op_ann', 'op_eve')
)
