SELECT status FROM payments WHERE order_id = 35 UNION ALL SELECT status FROM shipments WHERE cost < 32 ORDER BY status DESC
