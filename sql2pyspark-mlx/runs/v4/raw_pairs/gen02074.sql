SELECT id, order_id, carrier, shipped_date FROM shipments WHERE carrier = 'UPS' AND order_id IS NOT NULL ORDER BY shipped_date
