SELECT
    o.id,
    o.amount,
    o.customer_id,
    o.channel,
    c.is_active,
    c.segment,
    c.credit_limit
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'CANCELLED'
