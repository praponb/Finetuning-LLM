SELECT id, order_id, shipped_date, NTILE(3) OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS quartile FROM shipments;
