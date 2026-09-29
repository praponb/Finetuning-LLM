SELECT quantity, product_id, DATE_ADD(last_restock, 49) last_restock_plus_49d FROM inventory WHERE store_id = 6 ORDER BY quantity DESC
