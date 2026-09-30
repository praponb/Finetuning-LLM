SELECT orders.id, orders.amount, orders.status, stores.city FROM orders JOIN stores ON orders.store_id = stores.id
