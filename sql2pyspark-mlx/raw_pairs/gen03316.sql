WITH customer_stats AS (
    SELECT customer_id, MAX(discount) - MIN(discount) AS discount_range, COUNT(DISTINCT shipping_fee) AS unique_customers
    FROM orders
    WHERE channel IN ('store', 'web')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    cs.discount_range,
    cs.unique_customers
FROM customers c
INNER JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.discount_range >= 30
ORDER BY c.id
