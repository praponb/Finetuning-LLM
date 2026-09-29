WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 268) AS qty_distance,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'TRANSFER')
),
warehouse_id_stats AS (
    -- Avg qty_distance per warehouse_id
    SELECT warehouse_id, AVG(qty_distance) AS avg_qty_distance
    FROM filtered_moves
    GROUP BY warehouse_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_qty_distance,
        ROUND(s.qty_distance - g.avg_qty_distance, 2) AS qty_distance_minus_avg
    FROM filtered_moves s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.qty_distance > g.avg_qty_distance
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MAX(moved_at) AS last_moved_at,
        SUM(unit_price) AS total_move_unit_price,
        COUNT(*) FILTER (WHERE operator = 'op_dew') AS move_operator_count
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.total_move_unit_price,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM vs_group s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    operator,
    unit_price,
    qty_distance,
    operator_move_type_label,
    avg_qty_distance,
    qty_distance_minus_avg,
    total_move_unit_price,
    move_operator_count
FROM with_counts
ORDER BY qty_distance DESC
