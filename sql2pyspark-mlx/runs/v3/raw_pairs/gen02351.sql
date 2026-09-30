SELECT id, channel, order_date FROM orders WHERE customer_id IS NULL UNION SELECT id, channel, order_date FROM orders WHERE status = 'SHIPPED' ORDER BY channel;
