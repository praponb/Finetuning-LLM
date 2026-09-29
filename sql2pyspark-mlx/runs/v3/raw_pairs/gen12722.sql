SELECT p.id, p.supplier_id FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE LENGTH(supplier_name) >= 5) ORDER BY p.id DESC LIMIT 10;
