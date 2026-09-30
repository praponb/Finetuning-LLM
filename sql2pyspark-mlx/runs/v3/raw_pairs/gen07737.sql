WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY signup_date) AS pos
    FROM customers
)
SELECT *
FROM ranked
WHERE pos <= 1;
