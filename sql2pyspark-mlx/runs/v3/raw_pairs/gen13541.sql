SELECT product_id, COUNT(quantity) AS quantity_count, MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(AVG(quantity), 2) AS avg_quantity FROM order_items GROUP BY 1 ORDER BY 1 DESC LIMIT 5;
