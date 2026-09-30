SELECT product_id, last_restock FROM inventory WHERE store_id IN (1, 3, 4, 6) UNION ALL SELECT product_id, last_restock FROM inventory WHERE product_id = 2;
