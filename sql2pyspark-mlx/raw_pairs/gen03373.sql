WITH recent_suppliers AS (
    SELECT *, LOWER(country) AS country_lower
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
suppliers_stats AS (
    SELECT city, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM recent_suppliers
    GROUP BY city
)
SELECT city, stddev_rating
FROM suppliers_stats
WHERE stddev_rating < 61
ORDER BY stddev_rating DESC
