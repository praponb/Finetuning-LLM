SELECT
    orders.channel,
    customers.country,
    customers.email,
    stores.opened_date,
    stores.store_name
FROM orders
RIGHT OUTER JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id
WHERE stores.sqft > 4925 AND customers.signup_date > '2025-06-06';
