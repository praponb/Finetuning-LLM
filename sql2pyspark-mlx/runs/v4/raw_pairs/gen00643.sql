SELECT id, product_id, COUNT(*) OVER (PARTITION BY product_id) AS product_id_count FROM inventory;
