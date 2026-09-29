SELECT p.id, p.price, p.category FROM products p WHERE p.id IN (SELECT product_id FROM order_items) ORDER BY price DESC, p.category DESC, id ASC LIMIT 3
