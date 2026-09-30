SELECT id, shipped_date, NTILE(2) OVER (ORDER BY shipped_date DESC, id) AS shipped_date_ntile FROM shipments;
