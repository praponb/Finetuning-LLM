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
        qty + unit_price AS qty_plus_unit_price,
        unit_price - qty AS unit_price_minus_qty,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price > 150.7
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) FILTER (WHERE move_type IN ('TRANSFER', 'INBOUND', 'OUTBOUND')) AS move_move_type_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM scoped_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.last_moved_at,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.moved_at AS move_moved_at,
        st.move_type AS move_move_type
    FROM with_counts s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id <= 5
)
-- Subtotals with ROLLUP
SELECT
    move_type,
    warehouse_id,
    operator,
    COUNT(*) FILTER (WHERE move_type IN ('INBOUND', 'TRANSFER')) AS n_move_type_in_inbound_transfe
FROM exploded_detail
GROUP BY ROLLUP(move_type, warehouse_id, operator)
