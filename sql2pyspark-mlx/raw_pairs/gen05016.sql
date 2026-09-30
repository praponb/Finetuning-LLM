WITH matched_keys AS (
    SELECT country
    FROM suppliers
    INTERSECT
    SELECT country
    FROM suppliers
    WHERE rating > 2.2
),
kept_rows AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN matched_keys k ON s.country = k.country
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating
FROM kept_rows
ORDER BY city, country
