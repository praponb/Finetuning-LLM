SELECT p.id, p.product_name, p.brand FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE country = 'JP')
