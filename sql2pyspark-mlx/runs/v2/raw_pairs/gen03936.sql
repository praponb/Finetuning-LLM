SELECT id, status, shipped_date, NTILE(10) OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS quartile FROM shipments WHERE order_id IS NULL;
