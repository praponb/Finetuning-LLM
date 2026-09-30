WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty > 124
),
labeled AS (
    -- Label and score rows
    SELECT *, ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM prepared
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator IS NOT NULL) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count
    FROM labeled s
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price
    FROM with_stats s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_move_type,
    move_unit_price,
    move_unit_price - unit_price AS move_unit_price_minus_unit_price
FROM with_children
WHERE unit_price < 363.6
