WITH orders_by_customer AS (
    SELECT customer_id, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) returned_count
    FROM orders
    WHERE discount >= 0.31
    GROUP BY customer_id
)
SELECT c.id, c.name, obc.returned_count
FROM customers c
LEFT JOIN orders_by_customer obc ON c.id = obc.customer_id
ORDER BY c.name
