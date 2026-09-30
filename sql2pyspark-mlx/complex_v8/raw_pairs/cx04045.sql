WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        ROUND(weight_kg * 1.07, 2) AS weight_kg_adj
    FROM skus
    WHERE is_active = TRUE
      AND unit_cost < 277.0
)
SELECT id AS key_id, category
FROM recent
INTERSECT
SELECT capacity, city
FROM warehouses
WHERE city = 'Kuala Lumpur'
ORDER BY key_id
