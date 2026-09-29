SELECT id, product_id, last_restock, RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS last_restock_rank FROM inventory;
