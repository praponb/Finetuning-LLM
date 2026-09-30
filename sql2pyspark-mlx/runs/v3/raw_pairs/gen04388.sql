SELECT p.id, p.cost FROM products p WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id) ORDER BY p.id DESC, cost DESC LIMIT 10;
