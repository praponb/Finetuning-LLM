SELECT
    s.id,
    s.carrier,
    s.order_id,
    o.customer_id
FROM shipments s
RIGHT OUTER JOIN orders o ON s.order_id = o.id
WHERE s.carrier = 'DHL' AND o.status IN ('SHIPPED', 'COMPLETED', 'PENDING')
ORDER BY customer_id DESC
