WITH base_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        CASE WHEN unit_cost >= 251.2 THEN 'HIGH' WHEN unit_cost >= 134.3 THEN 'MEDIUM' ELSE 'LOW' END AS unit_cost_tier
    FROM skus
)
SELECT unit_cost_tier AS label, id, 'sku' AS record_type
FROM base_skus
UNION ALL
SELECT sku_code, id, 'sku_2' AS record_type
FROM skus
UNION ALL
SELECT operator, sku_id, 'move' AS record_type
FROM stock_moves
WHERE warehouse_id IS NOT NULL
ORDER BY label DESC
