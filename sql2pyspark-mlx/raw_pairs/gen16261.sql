WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
kept_keys AS (
    SELECT id, city
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, city
    FROM suppliers
    WHERE city = 'London'
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
kept_keys_2 AS (
    SELECT id
    FROM verified
    INTERSECT
    SELECT id
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys_2 k ON s.id = k.id
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        SUM(rating) OVER (PARTITION BY country) AS country_sum_rating,
        MIN(rating) OVER (PARTITION BY city) AS city_min_rating
    FROM reconciled
),
valid AS (
    SELECT
        id,
        supplier_name,
        country,
        country_sum_rating,
        city_min_rating,
        CONCAT(supplier_name, '/', country) AS supplier_name_country
    FROM with_windows
    WHERE rating BETWEEN 2.1 AND 4.0
),
valid_suppliers AS (
    SELECT
        country,
        city_min_rating,
        supplier_name_country,
        LENGTH(supplier_name) AS supplier_name_length
    FROM valid
    WHERE city_min_rating IS NOT NULL
)
SELECT country, city_min_rating
FROM valid_suppliers
ORDER BY country DESC, city_min_rating DESC
