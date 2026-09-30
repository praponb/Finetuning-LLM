SELECT
    p.id,
    p.status,
    o.shipping_fee,
    o.customer_id,
    c.signup_date
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
LEFT JOIN customers c ON o.customer_id = c.id
ORDER BY shipping_fee DESC, p.status DESC, id
LIMIT 1
