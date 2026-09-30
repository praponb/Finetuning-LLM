SELECT product_id, SUM(quantity) AS total_quantity, SUM(unit_price) AS total_unit_price FROM order_items GROUP BY product_id ORDER BY total_unit_price DESC
