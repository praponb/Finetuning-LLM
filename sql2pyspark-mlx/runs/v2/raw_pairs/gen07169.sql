SELECT id, unit_price, product_id, order_id FROM order_items WHERE unit_price >= (SELECT AVG(unit_price) FROM order_items) ORDER BY product_id, id DESC LIMIT 10;
