SELECT id, country, rating
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rnk
    FROM suppliers
) AS r
WHERE rnk <= 2
