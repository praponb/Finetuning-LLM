SELECT order_id, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range FROM order_items GROUP BY 1
