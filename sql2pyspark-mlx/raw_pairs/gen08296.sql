SELECT id, last_restock, store_id, COUNT(*) OVER (PARTITION BY MONTH(last_restock), store_id) AS last_restock_month_count FROM inventory WHERE quantity <= 239
