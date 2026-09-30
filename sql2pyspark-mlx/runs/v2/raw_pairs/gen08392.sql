SELECT p.id, p.price, p.product_name, p.brand FROM products p WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock >= '2024-10-01');
