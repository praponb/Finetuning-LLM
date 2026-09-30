SELECT
    orders.id,
    orders.store_id,
    orders.order_date,
    stores.store_name,
    customers.segment
FROM orders
LEFT OUTER JOIN stores ON orders.store_id = stores.id
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
