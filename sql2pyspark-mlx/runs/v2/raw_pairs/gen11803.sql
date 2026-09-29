SELECT
    s.id,
    s.shipped_date,
    o.shipping_fee,
    o.order_date,
    c.country,
    c.age
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
ORDER BY s.id DESC
LIMIT 20;
