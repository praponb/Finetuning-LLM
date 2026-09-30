WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
    WHERE city IN ('London', 'Berlin')
)
SELECT *
FROM ranked
WHERE rnk <= 5
