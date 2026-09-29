SELECT orders.order_date, orders.channel, stores.sqft, customers.segment FROM orders INNER JOIN stores ON orders.store_id = stores.id INNER JOIN customers ON orders.customer_id = customers.id
