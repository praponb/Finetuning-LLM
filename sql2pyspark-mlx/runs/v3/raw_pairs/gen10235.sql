SELECT product_id, ROUND(AVG(unit_price), 2) avg_unit_price FROM order_items GROUP BY product_id;
