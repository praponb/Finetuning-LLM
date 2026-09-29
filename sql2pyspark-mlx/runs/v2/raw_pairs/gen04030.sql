SELECT product_id, ROUND(AVG(quantity), 2) avg_quantity, SUM(quantity) total_quantity FROM inventory GROUP BY product_id;
