SELECT
    o.id,
    o.channel,
    o.store_id,
    c.country,
    c.credit_limit,
    c.is_active
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'COMPLETED' AND o.store_id = 7;
