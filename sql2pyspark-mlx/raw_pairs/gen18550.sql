WITH base AS (
    SELECT
        *,
        TRIM(country) AS clean_country,
        COALESCE(city, 'Unknown') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND city <> 'Chiang Mai'
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM products x
        WHERE x.supplier_id = s.id
          AND ARRAY_CONTAINS(x.tags, 'premium')
    );
