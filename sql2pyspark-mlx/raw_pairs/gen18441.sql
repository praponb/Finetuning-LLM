SELECT products.id, products.supplier_id FROM products WHERE products.category = 'Home' AND products.supplier_id IN (SELECT id FROM suppliers WHERE rating <= 2.1) ORDER BY id, supplier_id DESC
