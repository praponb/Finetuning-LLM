WITH customer_totals AS (
    SELECT customer_id, COUNT(DISTINCT amount) AS unique_stores
    FROM orders
    GROUP BY customer_id
)
SELECT c.credit_limit, c.email, ct.unique_stores
FROM customers c
INNER JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.unique_stores >= 8
