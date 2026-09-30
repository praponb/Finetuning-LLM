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
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE unit_price > 150.7
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price,
        s.least_qty_unit_price,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at
    FROM scoped_moves s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        MAX(unit_price) AS max_move_unit_price,
        MIN(qty) AS min_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.max_move_unit_price,
        st.min_move_qty
    FROM with_children s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM profiled
    WHERE move_move_type = 'OUTBOUND'
)
SELECT id, move_id, metric_name, value
FROM (
    SELECT id, move_id, unit_price, max_move_unit_price, least_qty_unit_price
    FROM enriched
) src
UNPIVOT (
    value FOR metric_name IN (unit_price, max_move_unit_price, least_qty_unit_price)
) unp
