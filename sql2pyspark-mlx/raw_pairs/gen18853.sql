SELECT *
FROM (
    SELECT city, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY city
) AS t
WHERE rating_range >= 38
ORDER BY rating_range ASC;
