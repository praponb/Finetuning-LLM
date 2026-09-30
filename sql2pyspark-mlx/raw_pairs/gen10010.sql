SELECT product_id, SUM(unit_price) AS total_unit_price FROM order_items GROUP BY product_id HAVING COUNT(DISTINCT quantity) >= 4
