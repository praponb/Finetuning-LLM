WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 15
      AND move_type = 'TRANSFER'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        qty_per_unit_price,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
    FROM prepared
    WHERE qty_per_unit_price <= 7.5
)
SELECT
    warehouse_id,
    sku_id,
    COUNT(*) AS row_count,
    MAX(qty_per_unit_price) AS max_qty_per_unit_price
FROM derived
GROUP BY GROUPING SETS ((warehouse_id, sku_id), (warehouse_id))
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
