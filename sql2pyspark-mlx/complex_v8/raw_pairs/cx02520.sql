WITH prepared AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        SUBSTRING(TRIM(sku_code), 1, 3) AS sku_code_prefix
    FROM skus
    WHERE unit_cost BETWEEN 17.3 AND 251.2
      AND is_active = TRUE
)
SELECT id
FROM prepared
WHERE sku_code IS NOT NULL
  AND category IS NOT NULL
INTERSECT ALL
SELECT id
FROM warehouses
WHERE wh_code IS NOT NULL
