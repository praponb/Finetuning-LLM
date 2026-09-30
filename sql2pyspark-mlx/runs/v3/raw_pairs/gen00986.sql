SELECT
    p.id,
    p.order_id,
    p.status,
    o.order_date,
    o.customer_id,
    c.country,
    c.is_active
FROM payments p
JOIN orders o ON p.order_id = o.id
JOIN customers c ON c.id = o.customer_id;
