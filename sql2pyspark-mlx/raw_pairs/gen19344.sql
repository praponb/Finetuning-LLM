SELECT order_date, 'order' AS origin
FROM orders
UNION
SELECT end_date, 'subscription' AS origin
FROM subscriptions
WHERE customer_id IN (35, 36)
ORDER BY origin DESC;
