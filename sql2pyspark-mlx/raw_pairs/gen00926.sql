SELECT
    id,
    country,
    rating,
    city,
    LAST_VALUE(city) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_city
FROM suppliers
WHERE rating > 2.1;
