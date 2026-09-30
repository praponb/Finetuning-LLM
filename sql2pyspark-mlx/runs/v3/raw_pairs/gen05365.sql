SELECT store_id FROM inventory WHERE MONTH(last_restock) IN (1, 5, 6) INTERSECT SELECT store_id FROM inventory WHERE product_id = 10;
