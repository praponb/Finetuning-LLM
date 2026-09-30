SELECT quantity, last_restock FROM inventory WHERE store_id = 6 EXCEPT SELECT quantity, last_restock FROM inventory WHERE product_id IN (2, 5)
