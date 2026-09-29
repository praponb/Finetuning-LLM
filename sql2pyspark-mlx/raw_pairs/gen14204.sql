SELECT id, unit_price, LEAST(unit_price, quantity) AS least_unit_price_quantity FROM order_items WHERE order_id IN (16, 38) AND (order_id IN (4, 31) OR quantity > 4)
