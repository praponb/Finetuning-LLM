SELECT id, product_id, quantity, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) quantity_dense_rank FROM order_items ORDER BY product_id
