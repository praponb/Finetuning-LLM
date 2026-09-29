SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory WHERE last_restock > '2024-02-01' AND last_restock < '2023-10-01' GROUP BY product_id ORDER BY product_id, avg_quantity DESC
