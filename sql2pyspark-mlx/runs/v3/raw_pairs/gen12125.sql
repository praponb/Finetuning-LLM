SELECT products.id, products.category, products.supplier_id FROM products WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE rating IS NOT NULL)
