SELECT id, quantity, last_restock, DATE_TRUNC('WEEK', last_restock) AS last_restock_period FROM inventory WHERE store_id IN (1, 2, 3, 4);
