SELECT product_id, SUM(unit_price) AS total_unit_price, SUM(SUM(unit_price)) OVER (ORDER BY product_id) AS cumulative_unit_price FROM order_items WHERE quantity >= 4 GROUP BY product_id
