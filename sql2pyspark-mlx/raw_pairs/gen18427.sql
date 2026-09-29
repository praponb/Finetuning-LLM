SELECT order_id, ROUND(AVG(quantity), 2) avg_quantity, MIN(quantity) min_quantity, COUNT(DISTINCT unit_price) unique_products FROM order_items GROUP BY order_id;
