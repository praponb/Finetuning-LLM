SELECT p.id, p.brand, p.price, p.product_name FROM products p WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.product_id = 6) ORDER BY p.brand, id LIMIT 1;
