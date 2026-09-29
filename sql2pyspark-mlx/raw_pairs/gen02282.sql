SELECT unit_price, order_id FROM order_items WHERE unit_price > 100 AND quantity <= 4 AND order_id IN (20, 25);
