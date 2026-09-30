SELECT
    o.id,
    o.customer_id,
    o.channel,
    c.email,
    c.name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id;
