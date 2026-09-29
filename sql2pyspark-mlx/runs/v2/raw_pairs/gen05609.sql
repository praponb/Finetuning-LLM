SELECT id, product_id, last_restock, PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY last_restock) AS last_restock_pct_rank FROM inventory WHERE product_id = 8;
