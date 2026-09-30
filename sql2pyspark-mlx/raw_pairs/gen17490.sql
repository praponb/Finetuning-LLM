SELECT
    o.customer_id,
    s.status,
    s.order_id,
    s.cost
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
WHERE o.status IN ('CANCELLED', 'PENDING', 'SHIPPED')
