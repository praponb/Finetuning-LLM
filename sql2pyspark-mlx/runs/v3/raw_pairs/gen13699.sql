WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS rnk
    FROM customers
)
SELECT *
FROM ranked
WHERE rnk <= 3
ORDER BY name
