WITH ranked_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) row_num
    FROM customers
)
SELECT id, city, age
FROM ranked_customers
WHERE row_num <= 3
ORDER BY age DESC;
