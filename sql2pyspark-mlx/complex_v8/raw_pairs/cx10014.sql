WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM skus
    WHERE sku_code IS NOT NULL
)
SELECT id AS ref_id, unit_cost
FROM cleaned
UNION ALL
SELECT sku_id, unit_price
FROM stock_moves
WHERE operator = 'op_eve'
ORDER BY unit_cost DESC
