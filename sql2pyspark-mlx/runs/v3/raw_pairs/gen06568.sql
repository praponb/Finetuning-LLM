SELECT id, shipped_date, NTILE(2) OVER (ORDER BY shipped_date, id) shipped_date_ntile
FROM shipments;
