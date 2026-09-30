SELECT order_id, carrier, shipped_date FROM shipments WHERE status NOT IN ('DELIVERED', 'IN_TRANSIT') AND status NOT IN ('RETURNED', 'IN_TRANSIT', 'DELIVERED');
