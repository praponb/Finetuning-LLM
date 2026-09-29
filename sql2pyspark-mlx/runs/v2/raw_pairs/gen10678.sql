SELECT payments.id, payments.paid_at, orders.channel, orders.amount, orders.customer_id FROM payments LEFT JOIN orders ON payments.order_id = orders.id;
