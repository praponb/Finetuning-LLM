SELECT
    p.id,
    p.method,
    p.amount,
    o.status,
    c.signup_date,
    c.city
FROM payments p
FULL OUTER JOIN orders o ON o.id = p.order_id
JOIN customers c ON c.id = o.customer_id
WHERE NOT (c.email IS NOT NULL AND o.shipping_fee > 20)
