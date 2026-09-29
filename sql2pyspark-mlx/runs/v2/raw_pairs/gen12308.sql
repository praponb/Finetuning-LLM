SELECT s.id, s.shipped_date, s.cost, s.order_id FROM shipments s WHERE s.shipped_date > '2024-05-01' AND s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee <= 14)
