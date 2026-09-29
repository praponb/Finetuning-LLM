SELECT id, shipped_date, status FROM shipments WHERE order_id IS NULL INTERSECT SELECT id, shipped_date, status FROM shipments WHERE status = 'RETURNED';
