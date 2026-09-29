SELECT product_id, SUM(unit_price) AS total_unit_price, AVG(quantity) AS avg_quantity FROM order_items WHERE unit_price >= 140 GROUP BY product_id ORDER BY 1 DESC;
