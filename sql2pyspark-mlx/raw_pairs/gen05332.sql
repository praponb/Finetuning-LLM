WITH order_stats AS (
    SELECT order_id, COUNT(DISTINCT amount) unique_methods
    FROM payments
    WHERE amount IS NOT NULL
    GROUP BY order_id
)
SELECT o.customer_id, o.amount, os.unique_methods
FROM orders o
JOIN order_stats os ON o.id = os.order_id
ORDER BY amount DESC;
