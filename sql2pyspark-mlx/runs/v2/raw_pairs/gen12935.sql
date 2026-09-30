SELECT id, status, shipped_date, DENSE_RANK() OVER (PARTITION BY status ORDER BY shipped_date) AS shipped_date_dense_rank FROM shipments WHERE order_id IS NOT NULL;
