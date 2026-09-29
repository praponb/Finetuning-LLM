SELECT status, channel, MAX(order_date) AS last_order_date FROM orders WHERE shipping_fee <= 29 GROUP BY status, channel ORDER BY last_order_date;
