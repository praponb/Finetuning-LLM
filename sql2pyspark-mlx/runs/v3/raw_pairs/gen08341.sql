SELECT order_id, product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, MAX(quantity) AS max_quantity FROM order_items GROUP BY order_id, product_id HAVING MAX(quantity) > 5.5;
