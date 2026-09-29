SELECT order_id, product_id, ROUND(AVG(quantity), 2) AS avg_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price FROM order_items GROUP BY order_id, product_id ORDER BY avg_unit_price;
