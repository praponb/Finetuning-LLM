WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        CASE WHEN qty >= 370 THEN 'large' WHEN qty >= 169 THEN 'medium' ELSE 'small' END AS qty_tier,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE qty < 300
      AND unit_price >= 61.2
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, ROUND(AVG(unit_price), 2) AS avg_move_unit_price
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price
    FROM valid s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
profiled_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty
    FROM profiled s
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.days_since_moved_at,
        s.qty_tier,
        s.unit_price_filled,
        s.move_count,
        s.total_move_qty,
        sk.category AS sku_category
    FROM profiled_2 s
    JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost > 158.7
)
SELECT *
FROM (
    SELECT sku_id, operator, days_since_moved_at
    FROM with_parent_sku
) src
PIVOT (
    MAX(days_since_moved_at)
    FOR operator IN ('op_dew', 'op_bee', 'op_eve', 'op_chan')
)
ORDER BY sku_id
