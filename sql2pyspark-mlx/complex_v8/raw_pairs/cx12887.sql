WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
)
SELECT id, category AS name
FROM scoped
WHERE weight_kg > 6.5
  AND is_active = TRUE
EXCEPT ALL
SELECT id, operator
FROM stock_moves
WHERE warehouse_id IS NOT NULL
ORDER BY name
