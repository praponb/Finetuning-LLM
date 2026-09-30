SELECT id, shipped_date FROM shipments WHERE order_id IS NULL UNION SELECT id, shipped_date FROM shipments WHERE cost BETWEEN 36 AND 92
