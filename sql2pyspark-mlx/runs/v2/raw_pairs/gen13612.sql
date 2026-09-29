SELECT order_date, channel, INITCAP(TRIM(channel)) AS channel_title FROM orders WHERE customer_id = 31 ORDER BY order_date;
