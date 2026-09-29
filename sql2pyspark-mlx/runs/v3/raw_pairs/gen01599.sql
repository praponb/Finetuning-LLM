SELECT id, status, carrier FROM shipments WHERE cost < 36 INTERSECT SELECT id, status, carrier FROM shipments WHERE status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED');
