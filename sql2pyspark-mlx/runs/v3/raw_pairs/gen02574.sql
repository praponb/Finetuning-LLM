SELECT
    orders.id,
    orders.status,
    stores.city,
    stores.region
FROM orders
LEFT JOIN stores ON orders.store_id = stores.id
ORDER BY orders.status;
