SELECT *
FROM (
    SELECT country, AVG(rating) AS avg_rating, SUM(rating) AS total_rating
    FROM suppliers
    GROUP BY country
) AS agg
WHERE total_rating < 13
ORDER BY country ASC;
