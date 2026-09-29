SELECT id, store_id, quantity, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_dense_rank FROM inventory ORDER BY store_id DESC, id
