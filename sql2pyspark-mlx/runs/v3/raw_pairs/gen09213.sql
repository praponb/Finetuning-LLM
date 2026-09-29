SELECT id, last_restock, DENSE_RANK() OVER (ORDER BY last_restock) AS last_restock_dense_rank FROM inventory WHERE store_id IN (1, 2, 3, 6) ORDER BY last_restock_dense_rank DESC;
