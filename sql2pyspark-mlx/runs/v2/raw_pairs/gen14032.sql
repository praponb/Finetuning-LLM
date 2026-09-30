SELECT
    orders.id,
    orders.store_id,
    orders.status,
    customers.signup_date,
    customers.is_active,
    stores.opened_date,
    stores.region
FROM orders
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id;
