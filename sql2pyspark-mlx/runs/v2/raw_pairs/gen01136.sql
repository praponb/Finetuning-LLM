SELECT payments.amount, payments.method, orders.amount AS order_amount FROM payments LEFT OUTER JOIN orders ON payments.order_id = orders.id WHERE orders.status <> 'CANCELLED'
