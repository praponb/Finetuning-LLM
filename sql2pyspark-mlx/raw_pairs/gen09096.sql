SELECT * FROM inventory WHERE (quantity <= 201 AND product_id = 6) OR MONTH(last_restock) IN (8, 10, 11);
