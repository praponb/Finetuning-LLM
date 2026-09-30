SELECT p.id, p.price, p.brand, p.stock_qty FROM products p WHERE p.supplier_id = 25 AND p.supplier_id IN (SELECT id FROM suppliers WHERE country = 'JP')
