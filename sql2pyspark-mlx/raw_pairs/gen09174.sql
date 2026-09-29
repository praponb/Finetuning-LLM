WITH top_customers AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS rnk
    FROM customers
)
SELECT
    id,
    country,
    age,
    signup_date
FROM top_customers
WHERE rnk <= 5;
