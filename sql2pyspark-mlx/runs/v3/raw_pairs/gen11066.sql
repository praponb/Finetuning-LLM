SELECT s.id, s.delivered_date
FROM shipments s
WHERE s.order_id IN (SELECT id FROM orders WHERE shipping_fee > 35)
ORDER BY s.id;
