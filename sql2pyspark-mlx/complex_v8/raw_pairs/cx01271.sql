WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 10:00:00'
      AND unit_price < 363.6
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        w.city AS warehouse_city
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, operator, warehouse_city, sku_unit_cost
    FROM without_matches
) src
PIVOT (
    MIN(sku_unit_cost)
    FOR warehouse_city IN ('Bangkok', 'Singapore')
)
ORDER BY sku_id, operator
