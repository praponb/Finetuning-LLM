WITH filtered_suppliers AS (
    SELECT *, LOWER(country) country_lower
    FROM suppliers
    WHERE city = 'Singapore' AND rating BETWEEN 2.9 AND 3.8 AND country = 'UK'
),
suppliers_stats AS (
    SELECT city, AVG(rating) avg_rating
    FROM filtered_suppliers
    GROUP BY city
)
SELECT city, avg_rating
FROM suppliers_stats
WHERE avg_rating < 2.9
ORDER BY city DESC;
