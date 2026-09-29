SELECT products.id, products.price FROM products WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name IS NOT NULL);
