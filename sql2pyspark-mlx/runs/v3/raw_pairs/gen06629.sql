WITH top_customers AS (
    SELECT
        id,
        segment,
        credit_limit,
        RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS pos
    FROM customers
    WHERE age < 45
)
SELECT id, segment, credit_limit
FROM top_customers
WHERE pos = 1
