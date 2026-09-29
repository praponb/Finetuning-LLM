SELECT country, city, rating_range
FROM (
    SELECT country, city, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY country, city
) AS x
WHERE rating_range < 46
ORDER BY rating_range DESC, city DESC, country
LIMIT 1;
