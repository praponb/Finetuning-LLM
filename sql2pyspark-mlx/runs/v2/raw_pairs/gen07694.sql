SELECT s.id, s.order_id, s.shipped_date
FROM shipments s
WHERE s.carrier IN ('FedEx', 'DHL', 'Flash') AND s.order_id NOT IN (SELECT id FROM orders WHERE status IN ('RETURNED', 'CANCELLED', 'SHIPPED'))
