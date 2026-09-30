SELECT id, country, age
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) pos
    FROM customers
) ranked
WHERE pos <= 2
ORDER BY 3
