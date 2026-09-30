SELECT MIN(order_date) AS min_order_date
FROM orders
WHERE (shipping_fee <= 13 AND customer_id = 20) OR amount > 1410;
