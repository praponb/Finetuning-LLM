WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
    WHERE qty > 89
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_city,
    warehouse_id,
    GROUPING(warehouse_city) AS is_total_warehouse_city,
    ROUND(STDDEV(sku_unit_cost), 2) AS stddev_sku_unit_cost,
    COUNT(DISTINCT operator) AS distinct_operators,
    PERCENTILE_APPROX(qty, 0.5) AS median_qty
FROM with_details_warehouse
GROUP BY GROUPING SETS ((warehouse_city, warehouse_id), (warehouse_city), (warehouse_id), ())
ORDER BY warehouse_city NULLS LAST, warehouse_id NULLS LAST
