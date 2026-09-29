SELECT id, channel FROM orders WHERE customer_id IN (1, 5, 26, 33) UNION SELECT id, channel FROM orders WHERE channel = 'store' ORDER BY channel;
