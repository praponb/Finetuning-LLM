SELECT
    s.id,
    s.delivered_date,
    s.carrier,
    o.shipping_fee,
    c.credit_limit,
    c.city
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE o.status IN ('RETURNED', 'CANCELLED', 'PENDING');
