SELECT payments.id, payments.method, payments.order_id, payments.amount, orders.channel FROM payments FULL OUTER JOIN orders ON orders.id = payments.order_id ORDER BY order_id DESC
