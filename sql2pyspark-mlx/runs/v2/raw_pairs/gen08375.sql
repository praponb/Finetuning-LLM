SELECT id, product_id, quantity, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS quantity_dense_rank FROM inventory ORDER BY product_id DESC;
