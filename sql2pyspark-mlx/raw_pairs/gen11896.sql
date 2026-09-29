SELECT s.* FROM products s WHERE EXISTS (SELECT 1 FROM order_items ch WHERE ch.product_id = s.id AND ch.unit_price BETWEEN 150 AND 230);
