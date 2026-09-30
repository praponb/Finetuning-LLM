SELECT orders.store_id, orders.order_date, stores.region, stores.store_name FROM orders LEFT JOIN stores ON stores.id = orders.store_id ORDER BY orders.store_id DESC
