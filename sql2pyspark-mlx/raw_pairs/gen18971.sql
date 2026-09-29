SELECT
    s.id,
    s.order_id,
    s.delivered_date,
    s.status
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('partner', 'mobile'))
