WITH base AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
suppliers_stats AS (
    SELECT city, MAX(rating) AS max_rating, SUM(CASE WHEN country = 'US' THEN rating ELSE 0 END) AS us_rating
    FROM base
    GROUP BY city
)
SELECT *
FROM suppliers_stats
ORDER BY city ASC
LIMIT 1;
