SELECT city, rating_count, rating_range
FROM (
    SELECT city, COUNT(rating) AS rating_count, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY city
) AS x
WHERE rating_count < 8
