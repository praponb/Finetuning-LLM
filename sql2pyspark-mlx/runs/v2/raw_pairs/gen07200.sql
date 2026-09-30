SELECT store_id, SUM(quantity) total_quantity, ROUND(AVG(quantity), 2) avg_quantity, MAX(last_restock) latest_last_restock FROM inventory GROUP BY store_id ORDER BY store_id DESC;
