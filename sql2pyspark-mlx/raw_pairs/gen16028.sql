SELECT s.* FROM orders s WHERE NOT EXISTS (SELECT 1 FROM order_items c WHERE c.order_id = s.id AND c.unit_price BETWEEN 200 AND 270);
