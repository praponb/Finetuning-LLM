SELECT product_id, MIN(unit_price) AS min_unit_price, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY product_id ORDER BY avg_unit_price DESC
