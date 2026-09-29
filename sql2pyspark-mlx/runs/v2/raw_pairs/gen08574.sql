SELECT
    o.id,
    o.store_id,
    o.channel,
    o.customer_id,
    c.country,
    c.is_active
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;
