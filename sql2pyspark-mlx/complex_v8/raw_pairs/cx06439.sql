WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_eve', 'op_chan')
              AND st.warehouse_id >= 5
        )
      AND s.sku_id IS NOT NULL
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.operator_slug,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id
    FROM without_matches s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_children_2 AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.operator_slug,
        s.move_id,
        s.move_move_type,
        s.move_qty,
        s.move_sku_id,
        st.id AS move_id_2,
        st.move_type,
        st.operator AS move_operator,
        st.qty AS move_qty_2,
        st.moved_at AS move_moved_at
    FROM with_children s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_id,
    move_move_type,
    move_qty,
    move_sku_id,
    move_type,
    move_operator,
    move_qty_2,
    move_moved_at
FROM with_children_2
