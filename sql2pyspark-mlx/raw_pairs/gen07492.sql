SELECT quantity FROM inventory WHERE product_id IN (1, 4, 6) EXCEPT SELECT quantity FROM inventory WHERE YEAR(last_restock) >= 2023
