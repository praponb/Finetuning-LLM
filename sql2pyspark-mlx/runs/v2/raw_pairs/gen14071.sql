SELECT id, store_id, quantity, NTILE(10) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_ntile FROM inventory
