SELECT product_id, quantity FROM order_items WHERE NOT (unit_price > 310 AND order_id = 17) ORDER BY product_id DESC;
