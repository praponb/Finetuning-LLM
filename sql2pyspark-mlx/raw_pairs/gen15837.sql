SELECT stores.region, stores.store_name, orders.customer_id
FROM stores
INNER JOIN orders ON orders.store_id = stores.id
ORDER BY store_name DESC
