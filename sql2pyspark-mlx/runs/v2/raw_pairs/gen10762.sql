SELECT payments.id, payments.method, payments.paid_at, orders.discount FROM payments INNER JOIN orders ON payments.order_id = orders.id WHERE payments.method = 'cash' AND orders.store_id IS NOT NULL;
