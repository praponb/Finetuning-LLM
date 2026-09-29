WITH payments_by_order AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_methods
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    o.channel,
    pbo.unique_methods
FROM orders o
INNER JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.unique_methods > 8
ORDER BY o.channel, id
LIMIT 10
