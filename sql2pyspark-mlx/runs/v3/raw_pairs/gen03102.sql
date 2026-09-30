SELECT id, unit_price, quantity, product_id, LEAST(unit_price, quantity) AS least_unit_price_quantity FROM order_items WHERE order_id = 30 ORDER BY quantity ASC
