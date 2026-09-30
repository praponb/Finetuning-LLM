SELECT
    orders.status,
    customers.signup_date,
    customers.segment,
    stores.opened_date
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id
WHERE LOWER(TRIM(stores.store_name)) = 'old town'
ORDER BY opened_date DESC, customers.signup_date;
