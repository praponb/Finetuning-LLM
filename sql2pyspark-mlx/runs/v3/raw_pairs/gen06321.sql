SELECT id, product_id, unit_price, quantity, ROUND(unit_price * 0.9, 2) AS adjusted_unit_price FROM order_items WHERE product_id IS NOT NULL
