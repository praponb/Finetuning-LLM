SELECT status, carrier FROM shipments WHERE cost > 31 UNION SELECT status, carrier FROM shipments WHERE order_id IN (17, 22, 35, 40);
