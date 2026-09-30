SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY brand ORDER BY price) AS rn
    FROM products
) AS ranked
WHERE rn <= 3
ORDER BY category DESC
