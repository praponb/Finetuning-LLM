SELECT *, DATE_TRUNC('WEEK', last_restock) AS last_restock_period, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE store_id IN (1, 2) ORDER BY quantity DESC;
