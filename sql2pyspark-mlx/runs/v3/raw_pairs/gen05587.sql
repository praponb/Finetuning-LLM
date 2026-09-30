SELECT p.id, p.price, p.brand, p.cost FROM products p WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.quantity < 62) ORDER BY p.cost DESC, price DESC
