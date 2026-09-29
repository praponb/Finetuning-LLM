SELECT
    shipments.id,
    shipments.status,
    orders.shipping_fee,
    orders.channel,
    customers.country,
    customers.credit_limit
FROM shipments
FULL OUTER JOIN orders ON shipments.order_id = orders.id
JOIN customers ON orders.customer_id = customers.id
WHERE NOT (orders.store_id IS NOT NULL OR orders.store_id = 13)
ORDER BY shipments.status
