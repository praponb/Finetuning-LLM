SELECT *
FROM (
    SELECT country, city, MIN(rating) min_rating
    FROM suppliers
    GROUP BY country, city
) agg
WHERE min_rating < 3.3
