SELECT product_id, store_id, MIN(last_restock) AS first_last_restock FROM inventory GROUP BY product_id, store_id ORDER BY first_last_restock, product_id;
