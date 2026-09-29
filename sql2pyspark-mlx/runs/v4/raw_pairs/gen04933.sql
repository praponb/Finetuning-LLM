SELECT id, last_restock, store_id, product_id FROM inventory WHERE last_restock = (SELECT MAX(last_restock) FROM inventory);
