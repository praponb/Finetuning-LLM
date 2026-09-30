SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(quantity) AS quantity_count FROM inventory GROUP BY product_id ORDER BY quantity_count ASC;
