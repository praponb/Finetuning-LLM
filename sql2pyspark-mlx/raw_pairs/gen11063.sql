SELECT p.id, p.price, p.category, p.supplier_id FROM products p WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.quantity IS NULL);
