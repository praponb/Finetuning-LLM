SELECT
    orders.store_id,
    orders.channel,
    customers.signup_date,
    stores.region
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id;
