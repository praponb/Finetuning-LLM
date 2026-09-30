WITH ranked_customers AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS row_num
    FROM customers
)
SELECT *
FROM ranked_customers
WHERE row_num = 1
ORDER BY row_num DESC;
