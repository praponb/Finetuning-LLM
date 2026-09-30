SELECT p.id, p.brand, p.product_name, p.price FROM products p WHERE p.supplier_id IN (9, 13, 26) AND p.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name LIKE 'Si%')
