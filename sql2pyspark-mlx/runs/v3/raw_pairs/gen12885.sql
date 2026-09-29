SELECT payments.id, payments.method, orders.status, orders.customer_id, stores.city FROM payments JOIN orders ON payments.order_id = orders.id RIGHT JOIN stores ON stores.id = orders.store_id;
