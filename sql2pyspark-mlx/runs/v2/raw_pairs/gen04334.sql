SELECT
    orders.customer_id,
    orders.order_date,
    orders.store_id,
    stores.region,
    stores.store_name
FROM orders
INNER JOIN stores ON orders.store_id = stores.id
WHERE orders.discount BETWEEN 0.14 AND 0.38 AND stores.opened_date < '2025-07-06'
