SELECT id, order_date, channel, FIRST_VALUE(channel) OVER (ORDER BY order_date DESC, id) AS first_channel FROM orders;
