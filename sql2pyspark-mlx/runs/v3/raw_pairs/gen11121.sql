SELECT products.id, products.supplier_id, products.category, products.product_name FROM products WHERE products.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'SG')
