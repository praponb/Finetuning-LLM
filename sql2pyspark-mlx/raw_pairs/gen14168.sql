SELECT
    id,
    city,
    sqft,
    region,
    NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS bucket,
    FIRST_VALUE(city) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS first_city
FROM stores
WHERE city IN ('Berlin', 'New York');
