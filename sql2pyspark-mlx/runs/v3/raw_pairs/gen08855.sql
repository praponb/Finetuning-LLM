SELECT order_id, MAX(quantity) - MIN(quantity) AS quantity_range, AVG(quantity) AS avg_quantity FROM order_items GROUP BY order_id ORDER BY order_id DESC, avg_quantity DESC LIMIT 10;
