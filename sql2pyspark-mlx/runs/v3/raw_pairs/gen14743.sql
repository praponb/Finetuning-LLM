SELECT orders.id, orders.store_id, orders.amount, orders.customer_id, stores.sqft, stores.region FROM orders JOIN stores ON orders.store_id = stores.id
