SELECT product_id, last_restock, store_id FROM inventory WHERE store_id IN (1, 4, 5, 6) ORDER BY store_id ASC, product_id DESC;
