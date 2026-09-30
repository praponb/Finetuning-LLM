SELECT quantity, product_id, store_id FROM inventory WHERE MONTH(last_restock) IN (4, 10, 11);
