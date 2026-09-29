SELECT store_id, product_id FROM inventory WHERE MONTH(last_restock) IN (1, 4, 11) UNION SELECT store_id, product_id FROM inventory WHERE quantity < 237;
