SELECT
    o.status,
    o.order_date,
    c.is_active,
    s.sqft,
    s.opened_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.channel <> 'partner'
ORDER BY sqft DESC
