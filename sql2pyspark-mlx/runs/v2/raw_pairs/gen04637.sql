SELECT p.id, p.supplier_id FROM products p WHERE p.category IN ('Grocery', 'Home') AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id) ORDER BY p.id;
