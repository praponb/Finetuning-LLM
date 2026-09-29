WITH ranked_customers AS (
    SELECT
        id,
        city,
        age,
        segment,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY age) rn
    FROM customers
)
SELECT *
FROM ranked_customers
WHERE rn <= 3
ORDER BY city
