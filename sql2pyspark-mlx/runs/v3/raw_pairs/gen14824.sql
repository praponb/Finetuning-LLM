SELECT p.id, p.supplier_id, p.category, p.price FROM products p WHERE p.id IN (SELECT product_id FROM inventory)
