SELECT
    orders.id,
    orders.status,
    orders.order_date,
    customers.name,
    stores.region
FROM orders
INNER JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id
WHERE orders.customer_id = 38
