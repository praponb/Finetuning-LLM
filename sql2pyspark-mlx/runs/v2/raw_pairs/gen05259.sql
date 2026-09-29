SELECT
    orders.discount,
    orders.status,
    customers.name,
    customers.credit_limit,
    stores.sqft,
    stores.store_name,
    orders.shipping_fee * orders.amount AS amount_with_shipping
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id
WHERE stores.sqft < 15875
