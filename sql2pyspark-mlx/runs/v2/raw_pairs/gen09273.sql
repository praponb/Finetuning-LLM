SELECT id, quantity FROM order_items WHERE quantity >= (SELECT AVG(quantity) FROM order_items WHERE unit_price >= 250) ORDER BY quantity DESC, id
