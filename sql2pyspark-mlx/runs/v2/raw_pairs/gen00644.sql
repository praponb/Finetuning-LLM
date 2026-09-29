SELECT * FROM inventory WHERE YEAR(last_restock) = 2025 AND store_id IN (1, 2, 3, 5) AND product_id = 7 ORDER BY 4 DESC, id LIMIT 20;
