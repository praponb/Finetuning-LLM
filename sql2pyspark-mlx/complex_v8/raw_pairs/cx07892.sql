WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.operator AS move_operator,
        st.move_type AS move_move_type
    FROM filtered_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Attach move attributes
    SELECT
        s.*,
        st.moved_at AS move_moved_at,
        st.move_type AS move_move_type_2,
        st.qty AS move_qty
    FROM exploded_detail_moves s
    LEFT JOIN stock_moves st ON st.id = s.move_id
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'ADJUST'
              AND st.moved_at >= TIMESTAMP '2025-02-06 08:00:00'
        )
      AND s.qty >= 198
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE move_qty > (SELECT MIN(move_qty) FROM filtered)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    move_id,
    move_operator,
    move_move_type,
    move_moved_at,
    move_move_type_2,
    move_qty,
    CASE WHEN move_type IN ('INBOUND', 'TRANSFER') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
    DATE_FORMAT(move_moved_at, 'yyyy-MM') AS move_moved_at_label
FROM with_activity
WHERE move_moved_at >= TIMESTAMP '2025-01-25 07:00:00'
ORDER BY move_type_group DESC, sku_id
