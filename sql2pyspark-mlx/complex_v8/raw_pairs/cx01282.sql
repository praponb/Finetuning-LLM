WITH recent_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active
    FROM skus
    WHERE weight_kg >= 13.6
)
SELECT id, unit_cost AS metric
FROM recent_skus
WHERE weight_kg > 8.5
  AND unit_cost BETWEEN 38.3 AND 306.4
UNION
SELECT qty, unit_price
FROM stock_moves
WHERE sku_id IS NOT NULL
ORDER BY metric, id
