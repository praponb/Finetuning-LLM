SELECT orders.id, orders.discount, orders.status, customers.email, stores.city FROM orders JOIN customers ON orders.customer_id = customers.id LEFT JOIN stores ON orders.store_id = stores.id
