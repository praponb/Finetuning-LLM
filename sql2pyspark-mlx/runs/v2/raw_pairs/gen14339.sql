SELECT order_date, channel
FROM orders
WHERE shipping_fee >= 14
UNION
SELECT order_date, channel
FROM orders
WHERE customer_id IN (10, 15, 25, 28);
