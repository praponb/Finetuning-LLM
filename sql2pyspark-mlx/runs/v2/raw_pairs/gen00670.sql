SELECT
    id,
    country,
    rating,
    city,
    FIRST_VALUE(city) OVER (PARTITION BY country ORDER BY rating DESC, id) AS first_city
FROM suppliers
WHERE city IN ('Berlin', 'London', 'New York')
