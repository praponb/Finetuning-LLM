SELECT
    s.id,
    s.delivered_date,
    o.store_id,
    c.signup_date,
    c.city
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.shipping_fee <= 15
ORDER BY city;
