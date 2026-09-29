SELECT p.id, p.supplier_id
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name IS NOT NULL);
