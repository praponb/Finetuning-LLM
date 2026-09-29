SELECT order_id, product_id, AVG(unit_price) AS avg_unit_price, SUM(quantity) AS total_quantity FROM order_items WHERE unit_price >= 140 GROUP BY order_id, product_id ORDER BY 4 DESC;
