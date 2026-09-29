WITH ranked_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS rn
    FROM customers
)
SELECT *
FROM ranked_customers
WHERE rn <= 3
