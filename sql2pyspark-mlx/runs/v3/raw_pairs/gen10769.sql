SELECT
    stores.region,
    stores.city,
    stores.store_name,
    orders.order_date
FROM stores
INNER JOIN orders ON stores.id = orders.store_id;
