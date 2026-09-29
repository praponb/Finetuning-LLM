SELECT id, store_id, last_restock, DATE_TRUNC('year', last_restock) last_restock_period FROM inventory WHERE store_id IN (2, 4, 5) AND MONTH(last_restock) IN (4, 7, 11) ORDER BY store_id;
