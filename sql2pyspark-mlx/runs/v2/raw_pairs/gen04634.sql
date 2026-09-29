SELECT country, rating_range
FROM (
    SELECT country, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY country
) AS x
WHERE rating_range >= 41
ORDER BY country, rating_range DESC
LIMIT 20
