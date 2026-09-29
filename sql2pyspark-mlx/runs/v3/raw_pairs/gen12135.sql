SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products FROM order_items WHERE unit_price >= 190 AND (order_id IN (5, 13, 14) OR order_id = 21) GROUP BY product_id ORDER BY product_id ASC;
