SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders, MAX(quantity) AS max_quantity FROM order_items GROUP BY 1 ORDER BY 2 DESC, order_id LIMIT 3;
