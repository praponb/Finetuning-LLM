SELECT s.id, s.shipped_date, s.order_id FROM shipments s WHERE s.carrier IN ('FedEx', 'DHL', 'Flash') AND s.order_id IN (SELECT id FROM orders WHERE shipping_fee < 32)
