WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
    WHERE qty <= 300
),
with_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    warehouse_id,
    sku_id,
    SUM(sku_unit_cost) AS total_sku_unit_cost
FROM with_details
GROUP BY GROUPING SETS ((warehouse_id, sku_id), (warehouse_id), (sku_id))
