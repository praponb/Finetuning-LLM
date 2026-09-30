SELECT start_date AS event_date
FROM subscriptions
WHERE start_date IS NOT NULL
EXCEPT
SELECT order_date AS event_date
FROM orders
WHERE order_date IS NOT NULL;
