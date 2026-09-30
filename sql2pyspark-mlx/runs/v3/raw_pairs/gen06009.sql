SELECT
    s.id,
    s.order_id,
    s.status,
    s.delivered_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE customer_id IS NULL);
