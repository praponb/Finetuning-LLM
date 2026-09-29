SELECT * FROM (SELECT id, store_id, last_restock, product_id, RANK() OVER (PARTITION BY store_id ORDER BY last_restock) rnk FROM inventory) t WHERE rnk = 1;
