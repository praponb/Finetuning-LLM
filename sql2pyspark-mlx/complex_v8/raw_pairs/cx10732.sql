WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        YEAR(moved_at) AS moved_at_year,
        YEAR(moved_at) AS moved_at_year_2
    FROM stock_moves
    WHERE warehouse_id <= 3
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.days_since_moved_at,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM base s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_details AS (
    -- Attach sku attributes
    SELECT s.*, sk.unit_cost AS sku_unit_cost
    FROM with_children s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(moved_at) AS first_moved_at,
        MIN(qty) AS min_move_qty,
        ROUND(AVG(qty), 2) AS avg_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.min_move_qty,
        st.avg_move_qty
    FROM with_details s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    id,
    sku_id,
    move_type,
    days_since_moved_at,
    move_id,
    move_move_type,
    move_qty,
    sku_unit_cost,
    min_move_qty,
    avg_move_qty,
    CASE WHEN move_move_type IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_move_type_group
FROM profiled
WHERE move_id >= 6
