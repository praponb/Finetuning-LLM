WITH base AS (
    SELECT *
    FROM stores
    WHERE store_name LIKE 'Air%' OR YEAR(opened_date) >= 2025
),
Agg AS (
    SELECT city, MAX(sqft) AS max_sqft, MIN(sqft) AS min_sqft
    FROM base
    GROUP BY city
)
SELECT *
FROM Agg
ORDER BY min_sqft DESC, city
