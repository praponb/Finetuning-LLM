WITH OrdersByCustomer AS (
    SELECT customer_id, COUNT(*) AS total_count
    FROM orders
    WHERE status IN ('CANCELLED', 'PENDING', 'SHIPPED')
    GROUP BY customer_id
)
SELECT c.id, c.email, O.total_count
FROM customers c
INNER JOIN OrdersByCustomer O ON c.id = O.customer_id
WHERE O.total_count > 9
ORDER BY total_count, id DESC
