SELECT id, channel, order_date, discount, LEAD(discount, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS next_discount FROM orders
