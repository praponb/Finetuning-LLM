SELECT
    id,
    city,
    rating,
    rating - LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS rating_change,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_num
FROM suppliers
WHERE city IN ('New York', 'London', 'Tokyo')
ORDER BY row_num DESC
