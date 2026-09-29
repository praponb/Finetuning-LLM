SELECT id, last_restock, product_id, store_id FROM inventory WHERE last_restock < '2024-04-01' ORDER BY last_restock DESC;
