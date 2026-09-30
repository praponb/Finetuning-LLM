SELECT
    orders.store_id,
    orders.amount,
    stores.region,
    customers.credit_limit,
    customers.name
FROM orders
JOIN stores ON orders.store_id = stores.id
FULL OUTER JOIN customers ON orders.customer_id = customers.id
WHERE stores.sqft != 12367
ORDER BY store_id DESC;
