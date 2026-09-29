SELECT orders.id, orders.channel, orders.status FROM orders WHERE orders.amount > 1450 AND orders.id IN (SELECT order_id FROM payments WHERE status = 'REFUNDED')
