SELECT id, order_date, channel, FLOOR(discount) AS discount_whole FROM orders WHERE status = 'CANCELLED'
