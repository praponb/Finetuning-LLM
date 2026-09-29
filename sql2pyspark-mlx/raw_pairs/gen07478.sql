SELECT start_date
FROM subscriptions
UNION ALL
SELECT order_date
FROM orders;
