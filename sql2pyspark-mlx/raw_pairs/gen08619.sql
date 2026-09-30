SELECT quantity, store_id, last_restock, DATE_SUB(last_restock, 48) last_restock_minus_48d FROM inventory WHERE store_id IN (3, 4, 5) AND product_id IN (2, 8, 9) ORDER BY last_restock_minus_48d;
