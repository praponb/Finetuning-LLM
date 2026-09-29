SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price FROM order_items GROUP BY product_id
