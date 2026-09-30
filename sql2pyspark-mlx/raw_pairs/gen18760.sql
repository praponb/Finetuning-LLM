WITH customer_stats AS (
    SELECT customer_id, MIN(amount) min_amount, MAX(amount) - MIN(amount) amount_range
    FROM orders
    WHERE customer_id IN (3, 5, 28)
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.email,
    customers.segment,
    customer_stats.min_amount,
    customer_stats.amount_range
FROM customers
LEFT JOIN customer_stats ON customers.id = customer_stats.customer_id
WHERE customer_stats.amount_range > 50
ORDER BY segment;
