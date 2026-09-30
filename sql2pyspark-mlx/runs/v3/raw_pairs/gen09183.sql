SELECT id, unit_price, product_id, unit_price * quantity AS line_total FROM order_items WHERE product_id IN (4, 7) AND quantity > 5;
