SELECT
    o.order_date,
    o.channel,
    o.shipping_fee,
    c.name,
    c.is_active,
    c.age
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.email LIKE '%@outlook.com'
