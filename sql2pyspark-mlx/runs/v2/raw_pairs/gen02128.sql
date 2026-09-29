SELECT *, DATE_TRUNC('WEEK', last_restock) AS last_restock_period FROM inventory WHERE store_id = 3 AND store_id IN (2, 6)
