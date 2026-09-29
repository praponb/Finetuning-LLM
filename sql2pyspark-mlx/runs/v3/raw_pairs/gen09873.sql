SELECT
    p.id,
    p.paid_at,
    o.shipping_fee,
    o.channel,
    c.city,
    c.signup_date
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
JOIN customers c ON o.customer_id = c.id
ORDER BY 2;
