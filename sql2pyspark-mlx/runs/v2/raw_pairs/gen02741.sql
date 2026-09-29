SELECT stores.sqft, stores.store_name, orders.status, orders.store_id, orders.discount FROM stores LEFT OUTER JOIN orders ON stores.id = orders.store_id
