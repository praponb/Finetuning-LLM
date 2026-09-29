SELECT * FROM (SELECT store_id, AVG(quantity) AS avg_quantity, MAX(last_restock) AS last_last_restock FROM inventory GROUP BY store_id) AS t WHERE avg_quantity > 160 ORDER BY avg_quantity DESC;
