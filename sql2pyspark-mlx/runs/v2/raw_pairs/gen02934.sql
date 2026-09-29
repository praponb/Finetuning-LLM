SELECT order_id, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY order_id HAVING avg_unit_price >= 320 ORDER BY avg_unit_price DESC, order_id LIMIT 10
