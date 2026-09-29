SELECT id, order_id, LOWER(status) AS status_lower FROM shipments WHERE order_id IS NOT NULL;
