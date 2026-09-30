SELECT id, shipped_date, NTILE(10) OVER (ORDER BY shipped_date DESC, id) AS bucket FROM shipments ORDER BY shipped_date
