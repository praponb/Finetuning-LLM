SELECT id, store_id, quantity, NTILE(4) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quartile FROM inventory
