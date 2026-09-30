WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        CASE WHEN unit_price >= 363.6 THEN 'HIGH' WHEN unit_price >= 136.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE (warehouse_id IS NOT NULL OR sku_id >= 16)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        MAX(moved_at) AS last_moved_at,
        MAX(qty) AS max_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty,
        st.last_moved_at,
        st.max_move_qty
    FROM cleaned s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
combined_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.least_unit_price_qty,
        s.unit_price_tier,
        s.avg_move_qty,
        s.last_moved_at,
        s.max_move_qty,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM with_stats s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.unit_cost BETWEEN 108.5 AND 277.0
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_category, operator, sku_unit_cost
    FROM combined_details
) src
PIVOT (
    SUM(sku_unit_cost)
    FOR operator IN ('op_bee', 'op_chan', 'op_ann')
)
ORDER BY sku_category
