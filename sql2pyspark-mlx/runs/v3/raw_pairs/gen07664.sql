SELECT city, total_rating
FROM (
    SELECT city, SUM(rating) AS total_rating
    FROM suppliers
    GROUP BY city
) AS s
WHERE total_rating < 11
ORDER BY total_rating DESC;
