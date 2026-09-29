WITH customer_stats AS (
    SELECT customer_id, MIN(order_date) AS min_order_date
    FROM orders
    WHERE discount BETWEEN 0.12 AND 0.31
    GROUP BY customer_id
)
SELECT c.city, cs.min_order_date
FROM customers c
JOIN customer_stats cs ON c.id = cs.customer_id
ORDER BY min_order_date DESC;
