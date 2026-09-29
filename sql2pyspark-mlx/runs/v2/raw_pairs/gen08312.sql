SELECT status FROM shipments WHERE status = 'DELIVERED' UNION SELECT status FROM shipments WHERE order_id IS NULL
