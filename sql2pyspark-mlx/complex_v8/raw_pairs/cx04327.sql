WITH scoped_skus AS (
    -- Scope the skus rows
    SELECT *
    FROM skus
)
SELECT id AS entity_id, sku_code
FROM scoped_skus
UNION
SELECT capacity, city
FROM warehouses
