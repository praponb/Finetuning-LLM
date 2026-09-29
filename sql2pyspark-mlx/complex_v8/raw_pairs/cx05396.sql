WITH valid AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE (unit_price >= 161.4 OR move_type = 'OUTBOUND')
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) FILTER (WHERE move_type NOT IN ('TRANSFER', 'INBOUND')) AS move_move_type_count
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM valid s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM profiled s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Label and score rows
    SELECT *, FLOOR(move_qty / 100) * 100 AS move_qty_bucket
    FROM with_children_moves
    WHERE move_id >= 59
)
SELECT
    move_warehouse_id,
    move_operator,
    sku_id,
    GROUPING(move_warehouse_id) AS is_total_move_warehouse_id,
    ROUND(AVG(move_qty_bucket), 2) AS avg_move_qty_bucket
FROM enriched
GROUP BY ROLLUP(move_warehouse_id, move_operator, sku_id)
ORDER BY move_warehouse_id NULLS LAST, move_operator NULLS LAST, sku_id NULLS LAST
