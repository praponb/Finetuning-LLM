SELECT id, carrier, shipped_date, order_id FROM shipments WHERE order_id IN (5, 7, 14, 38) AND order_id IS NOT NULL AND status = 'DELIVERED';
