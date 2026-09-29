SELECT product_id, MIN(last_restock) AS first_last_restock, AVG(quantity) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range FROM inventory GROUP BY product_id ORDER BY 2;
