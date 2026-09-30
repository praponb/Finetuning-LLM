SELECT carrier, delivered_date FROM shipments WHERE status = 'IN_TRANSIT' INTERSECT SELECT carrier, delivered_date FROM shipments WHERE status IN ('DELIVERED', 'RETURNED');
