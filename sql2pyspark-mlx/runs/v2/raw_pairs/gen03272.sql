SELECT * FROM (SELECT order_id, SUM(unit_price) AS total_unit_price, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY order_id) AS t WHERE avg_unit_price >= 150
