SELECT country, rating_count
FROM (
    SELECT country, COUNT(rating) AS rating_count
    FROM suppliers
    WHERE city = 'Singapore'
    GROUP BY country
) AS x
WHERE rating_count > 4
ORDER BY rating_count ASC;
