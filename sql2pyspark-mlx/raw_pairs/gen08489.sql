SELECT
    id,
    city,
    age,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY age DESC) AS dense_rnk
FROM customers
WHERE city IS NOT NULL;
