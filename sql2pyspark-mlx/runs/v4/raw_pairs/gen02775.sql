SELECT id, quantity FROM order_items WHERE quantity > (SELECT MAX(quantity) FROM order_items) ORDER BY id DESC
