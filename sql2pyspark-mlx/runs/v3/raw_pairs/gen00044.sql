SELECT
    orders.id,
    orders.discount,
    stores.opened_date,
    customers.segment,
    LOWER(stores.region) AS region_lower
FROM orders
INNER JOIN stores ON stores.id = orders.store_id
INNER JOIN customers ON orders.customer_id = customers.id
ORDER BY region_lower ASC, id ASC
LIMIT 10;
