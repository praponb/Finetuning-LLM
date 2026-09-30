SELECT s.id, s.delivered_date, s.shipped_date, s.status FROM shipments s WHERE s.carrier = 'DHL' AND s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee > 29);
