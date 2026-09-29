WITH top_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS row_num
    FROM customers
)
SELECT *
FROM top_customers
WHERE row_num <= 3
ORDER BY city DESC, row_num DESC;
