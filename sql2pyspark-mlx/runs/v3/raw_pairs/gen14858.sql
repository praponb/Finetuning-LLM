SELECT *, DAYOFWEEK(order_date) AS order_date_dayofweek
FROM orders
WHERE customer_id IS NULL;
