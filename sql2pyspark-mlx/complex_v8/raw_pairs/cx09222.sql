WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE sku_id <= 19
      AND warehouse_id <= 5
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.moved_at_plus_7d,
        s.least_qty_unit_price,
        sk.weight_kg AS sku_weight_kg
    FROM recent s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    warehouse_id,
    sku_id,
    ROUND(AVG(least_qty_unit_price), 2) AS avg_least_qty_unit_price,
    MIN(moved_at_plus_7d) AS first_moved_at_plus_7d
FROM with_details
GROUP BY GROUPING SETS ((warehouse_id, sku_id), (warehouse_id), ())
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
