SELECT
    id,
    city,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS row_num
FROM stores
WHERE city IN ('Berlin', 'Tokyo', 'London')
