SELECT orders.store_id, customers.is_active FROM orders INNER JOIN customers ON orders.customer_id = customers.id ORDER BY orders.store_id DESC
