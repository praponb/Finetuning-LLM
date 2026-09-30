SELECT
    orders.id,
    orders.order_date,
    orders.channel,
    orders.amount,
    stores.region,
    UPPER(orders.status) status_upper
FROM orders
LEFT JOIN stores ON orders.store_id = stores.id
WHERE orders.customer_id IN (8, 10, 11) AND stores.sqft <> 6157
