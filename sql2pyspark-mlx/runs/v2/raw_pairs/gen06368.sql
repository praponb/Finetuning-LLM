SELECT *
FROM (
    SELECT country, MAX(rating) AS max_rating, SUM(rating) AS total_rating
    FROM suppliers
    WHERE city IS NULL
    GROUP BY country
) AS x
WHERE max_rating >= 2.8
