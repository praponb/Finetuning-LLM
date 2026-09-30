SELECT order_id, SUM(unit_price) total_unit_price, AVG(quantity) avg_quantity FROM order_items GROUP BY order_id HAVING avg_quantity >= 3.5
