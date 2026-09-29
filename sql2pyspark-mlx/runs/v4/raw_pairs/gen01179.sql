SELECT id, channel, order_date, discount, LAG(discount, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS prev_discount FROM orders ORDER BY order_date;
