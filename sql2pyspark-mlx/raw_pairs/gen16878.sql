WITH ranked_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) rn
    FROM customers
)
SELECT *
FROM ranked_customers
WHERE rn <= 2
ORDER BY name DESC
