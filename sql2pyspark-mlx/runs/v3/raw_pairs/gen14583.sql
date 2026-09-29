SELECT channel, store_id, customer_id, CONCAT(channel, '/', status) channel_status FROM orders WHERE MONTH(order_date) IN (4, 8, 12) AND channel = 'partner' ORDER BY customer_id DESC;
