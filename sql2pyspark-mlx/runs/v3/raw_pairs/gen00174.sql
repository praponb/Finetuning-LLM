SELECT products.id, products.category FROM products WHERE SIZE(products.tags) > 2 AND products.supplier_id IN (SELECT id FROM suppliers WHERE country = 'SG');
