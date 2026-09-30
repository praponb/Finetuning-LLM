SELECT status FROM shipments WHERE cost <= 30 UNION SELECT status FROM shipments WHERE order_id IS NOT NULL ORDER BY status;
