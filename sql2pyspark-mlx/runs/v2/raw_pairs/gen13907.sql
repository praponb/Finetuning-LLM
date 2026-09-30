SELECT
    c.is_active,
    c.credit_limit,
    o.shipping_fee,
    o.channel,
    o.discount
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'RETURNED' AND o.shipping_fee < 28
ORDER BY o.discount DESC, o.channel;
