SELECT order_id, MIN(unit_price) AS min_unit_price FROM order_items WHERE order_id IN (13, 29) AND product_id IS NULL GROUP BY order_id ORDER BY order_id DESC LIMIT 5
