SELECT id, country, age
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS rn
    FROM customers
    WHERE is_active = TRUE
) AS r
WHERE rn <= 1;
