SELECT status, channel, MIN(order_date) min_order_date
FROM orders
WHERE customer_id = 35
GROUP BY 1, 2;
