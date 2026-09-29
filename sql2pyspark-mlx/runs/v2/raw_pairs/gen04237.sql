WITH ranked_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS pos
    FROM customers
)
SELECT
    id,
    segment,
    credit_limit,
    age
FROM ranked_customers
WHERE pos <= 2
ORDER BY segment DESC;
