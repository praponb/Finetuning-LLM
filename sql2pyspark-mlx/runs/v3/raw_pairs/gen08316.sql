SELECT order_date
FROM orders
EXCEPT
SELECT start_date
FROM subscriptions
WHERE MONTH(end_date) IN (1, 6, 12)
