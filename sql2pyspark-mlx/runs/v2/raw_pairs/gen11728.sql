SELECT last_restock, product_id FROM inventory WHERE quantity >= 146 INTERSECT SELECT last_restock, product_id FROM inventory WHERE store_id IN (1, 4, 5, 6) ORDER BY last_restock DESC
