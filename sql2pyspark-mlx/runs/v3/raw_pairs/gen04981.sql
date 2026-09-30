SELECT id, order_id, quantity, unit_price, ROUND(unit_price * 0.85, 2) AS adjusted_unit_price FROM order_items
