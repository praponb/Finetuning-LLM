SELECT id, store_id, discount, NTILE(10) OVER (PARTITION BY store_id ORDER BY discount DESC, id) AS bucket FROM orders;
