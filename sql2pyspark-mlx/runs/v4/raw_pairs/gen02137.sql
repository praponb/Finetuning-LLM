WITH customer_summary AS (
    SELECT customer_id, SUM(discount) AS total_discount
    FROM orders
    GROUP BY customer_id
)
SELECT c.id, c.city, cs.total_discount
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.total_discount >= 2.0
