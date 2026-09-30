SELECT id, quantity, product_id, NTILE(3) OVER (ORDER BY quantity, id) quartile, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) quantity_rank FROM inventory;
