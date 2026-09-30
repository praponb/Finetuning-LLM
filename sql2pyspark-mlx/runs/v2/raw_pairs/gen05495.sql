SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, AVG(quantity) AS avg_quantity, MIN(quantity) AS min_quantity FROM order_items GROUP BY order_id
