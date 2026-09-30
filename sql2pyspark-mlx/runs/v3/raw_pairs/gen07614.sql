SELECT orders.id, orders.channel, orders.order_date, orders.amount FROM orders WHERE orders.id IN (SELECT order_id FROM payments)
