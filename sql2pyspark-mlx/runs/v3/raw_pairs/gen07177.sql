SELECT
    orders.id,
    orders.customer_id,
    orders.channel,
    stores.store_name,
    stores.city
FROM orders
LEFT JOIN stores ON orders.store_id = stores.id
WHERE stores.city <> 'Singapore' AND stores.region NOT IN ('West', 'North', 'Central')
ORDER BY stores.city ASC;
