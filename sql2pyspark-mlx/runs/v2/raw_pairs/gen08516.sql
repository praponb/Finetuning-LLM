SELECT id, unit_price, quantity, product_id FROM order_items WHERE unit_price > (SELECT MAX(unit_price) FROM order_items);
