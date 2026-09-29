WITH valid AS (
    SELECT *, CONCAT_WS('/', city, country) AS city_country_key
    FROM suppliers
    WHERE city != 'New York'
)
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM products c
        WHERE c.supplier_id = s.id
          AND ARRAY_CONTAINS(c.tags, 'bestseller')
    )
