SELECT id, customer_id, channel FROM orders WHERE discount <= 0.4 ORDER BY customer_id, channel DESC;
