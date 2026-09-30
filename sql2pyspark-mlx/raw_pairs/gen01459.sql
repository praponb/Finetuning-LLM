SELECT
    o.id,
    o.customer_id,
    o.shipping_fee,
    o.discount,
    c.email,
    c.is_active,
    c.signup_date
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE NOT (c.credit_limit > 9400 AND o.channel IN ('web', 'partner'));
