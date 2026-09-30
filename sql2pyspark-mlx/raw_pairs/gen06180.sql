WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS rn
    FROM customers
    WHERE city = 'Bangkok'
)
SELECT id, segment, credit_limit
FROM ranked
WHERE rn = 1;
