SELECT
    s.id,
    s.order_id,
    s.status,
    s.shipped_date
FROM shipments s
WHERE s.order_id IS NULL AND s.order_id IN (SELECT id FROM orders WHERE status IN ('COMPLETED', 'SHIPPED'))
