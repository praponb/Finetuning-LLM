SELECT id, order_id, delivered_date FROM shipments WHERE cost >= 85 UNION SELECT id, order_id, delivered_date FROM shipments WHERE carrier = 'Flash';
