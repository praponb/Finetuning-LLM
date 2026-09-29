SELECT id, quantity, product_id, unit_price FROM order_items WHERE quantity > (SELECT MAX(quantity) FROM order_items)
