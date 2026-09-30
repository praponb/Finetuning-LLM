WITH base_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        unit_cost + weight_kg AS unit_cost_plus_weight_kg
    FROM skus
    WHERE (UPPER(TRIM(sku_code)) LIKE 'T%' OR NOT ARRAY_CONTAINS(labels, 'cold'))
)
SELECT id, sku_code
FROM base_skus
EXCEPT ALL
SELECT id, operator
FROM stock_moves
WHERE unit_price < 399.6
  AND MONTH(moved_at) IN (2, 3)
ORDER BY sku_code, id
