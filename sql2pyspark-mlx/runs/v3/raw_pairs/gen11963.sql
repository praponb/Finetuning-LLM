SELECT
    p.id,
    p.method,
    p.order_id,
    o.status,
    c.name,
    c.segment
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
